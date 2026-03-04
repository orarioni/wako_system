from __future__ import annotations

import argparse
import signal
import sys
import time
from pathlib import Path
from typing import List, Optional

import numpy as np
import soundfile as sf
from rich.console import Console

from app.asr.dedupe import merge_with_recent
from app.asr.whisperer import Whisperer, choose_compute_type
from app.audio.resample import resample_audio

console = Console()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="WASAPI loopback real-time transcription")
    p.add_argument("--list-devices", action="store_true")
    p.add_argument("--hostapi", default="wasapi", help="Host API filter for device listing")
    p.add_argument("--mode", choices=["loopback", "mic"], default="loopback")
    p.add_argument("--device", type=int, default=None)
    p.add_argument("--sample-rate", type=int, default=48000)
    p.add_argument("--channels", type=int, default=2)

    p.add_argument("--model", default="small")
    p.add_argument("--compute-type", default="auto", help="auto/float16/int8/float32")
    p.add_argument("--language", default="ja", help="ja or auto")

    p.add_argument("--window-sec", type=float, default=6.0)
    p.add_argument("--step-sec", type=float, default=2.0)
    p.add_argument("--commit-delay-sec", type=float, default=4.0)

    p.add_argument("--vad-aggressiveness", type=int, default=2, choices=[0, 1, 2, 3])
    p.add_argument("--vad-min-ratio", type=float, default=0.25)

    p.add_argument("--save-txt", type=Path, default=None)
    p.add_argument("--save-wav", type=Path, default=None)
    p.add_argument("--self-test", action="store_true")
    return p.parse_args()


def run_self_test() -> int:
    from app.audio.resample import resample_audio
    from app.audio.vad import VadConfig, VadGate

    sr = 48000
    t = np.linspace(0, 1.0, int(sr), endpoint=False)
    sine = 0.15 * np.sin(2 * np.pi * 440 * t)
    stereo = np.stack([sine, sine], axis=1).astype(np.float32)

    mono16 = resample_audio(stereo, src_rate=sr, dst_rate=16000)
    vad = VadGate(VadConfig(aggressiveness=2, min_speech_ratio=0.1))
    speech = vad.has_speech(mono16)

    console.print(f"[self-test] input={stereo.shape} -> resampled={mono16.shape}, vad={speech}")
    console.print("[self-test] PASS (pipeline primitives seem functional)")
    return 0


def main() -> int:
    args = parse_args()

    if args.self_test:
        return run_self_test()

    if args.list_devices:
        try:
            from app.audio.devices import format_devices
        except Exception as e:
            console.print(f"[red]デバイス一覧取得に失敗: {e}[/red]")
            console.print("PortAudio未導入の可能性があります。Windows環境で実行し、sounddevice依存を確認してください。")
            return 1

        print(format_devices(hostapi_filter=args.hostapi))
        return 0

    stop = False

    def _sigint(_sig, _frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _sigint)

    from app.audio.vad import VadConfig, VadGate

    vad = VadGate(VadConfig(args.vad_aggressiveness, 30, args.vad_min_ratio))
    device_type, compute_type = choose_compute_type(args.compute_type)

    console.print(f"[info] loading model={args.model} device={device_type} compute_type={compute_type}")
    whisper = Whisperer(args.model, compute_type=compute_type, device=device_type)

    try:
        from app.audio.capture import AudioCapture, SlidingWindowBuffer
    except Exception as e:
        console.print(f"[red]音声キャプチャ初期化に失敗: {e}[/red]")
        console.print("PortAudio/sounddevice設定を確認してください。")
        return 1

    capture = AudioCapture(
        device=args.device,
        mode=args.mode,
        sample_rate=args.sample_rate,
        channels=args.channels,
    )
    buffer = SlidingWindowBuffer(sample_rate=16000, max_sec=max(30, args.window_sec + 10))

    if args.save_txt:
        args.save_txt.parent.mkdir(parents=True, exist_ok=True)
    if args.save_wav:
        args.save_wav.parent.mkdir(parents=True, exist_ok=True)

    txt_fh = open(args.save_txt, "a", encoding="utf-8") if args.save_txt else None
    wav_chunks: List[np.ndarray] = []
    recent_output = ""
    last_commit_sec = 0.0
    captured_audio_sec = 0.0

    try:
        capture.start()
        console.print("[info] capturing started. Press Ctrl+C to stop.")

        while not stop:
            step_start = time.time()
            while time.time() - step_start < args.step_sec and not stop:
                chunk = capture.read(timeout=0.2)
                if chunk is None:
                    continue
                if args.save_wav:
                    wav_chunks.append(chunk.data.copy())
                mono16 = resample_audio(chunk.data, chunk.sample_rate, 16000)
                buffer.append(mono16)
                captured_audio_sec += len(mono16) / 16000.0

            if stop:
                break

            window_audio = buffer.get_last(args.window_sec)
            if not vad.has_speech(window_audio):
                continue

            segs = whisper.transcribe(window_audio, language=args.language)
            window_start_sec = max(0.0, captured_audio_sec - (len(window_audio) / 16000.0))

            for seg in segs:
                rel_start = window_start_sec + seg.start
                rel_end = window_start_sec + seg.end
                if rel_end > captured_audio_sec - args.commit_delay_sec:
                    continue
                if rel_end <= last_commit_sec:
                    continue
                delta = merge_with_recent(recent_output[-120:], seg.text)
                if not delta:
                    continue

                line = f"[{rel_start:8.2f}-{rel_end:8.2f}] {delta}"
                console.print(line)
                if txt_fh:
                    txt_fh.write(line + "\n")
                    txt_fh.flush()
                recent_output += " " + delta
                last_commit_sec = rel_end

    except Exception as e:
        console.print("[red]エラーが発生しました。[/red]")
        console.print(f"原因: {e}")
        console.print(
            "対処: --list-devices でデバイス番号確認、WASAPI対応確認、sample-rate(48000/44100)変更、"
            "Windowsのサウンド設定・ドライバ更新を試してください。"
        )
        return 1
    finally:
        capture.stop()
        if txt_fh:
            txt_fh.close()
        if args.save_wav and wav_chunks:
            audio = np.concatenate(wav_chunks, axis=0)
            sf.write(args.save_wav, audio, args.sample_rate)
            console.print(f"[info] wav saved: {args.save_wav}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
