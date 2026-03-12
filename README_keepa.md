# Keepa 月販推定付与ツール

## 機能概要
`output.xlsx` を読み込み、Keepa API から `monthlySold` / `stats.salesRankDrops30` を取得して、月販推定列を追加した `output_keepa.xlsx` を出力します。

主な仕様:
- **元の全行を保持**して出力（ASIN 空欄行も削除しない）
- 差分更新キューにより、Keepa API 呼び出し対象は **今回取得が必要な ASIN のみ**
- 今回取得しない ASIN でも、ローカルキャッシュ（`asin_cache.csv`）の過去データを出力に反映
- ASIN 空欄行は推定不可として以下を設定
  - `estimate_source = unavailable`
  - `estimate_confidence = D`
  - `estimate_note = ASIN missing`
  - Keepa由来列と推定値列は空欄
- `monthlySold` を最優先で推定値に採用
- `monthlySold` が無い場合は `salesRankDrops30 × 補正係数` で推定
- 補正係数は `monthlySold >= 1` かつ `salesRankDrops30 >= 1` の `monthlySold / salesRankDrops30` の中央値
- 係数が算出できない場合は `1.0`
- 一部 ASIN で API 失敗があっても全体処理は継続


## 差分更新キューとキャッシュ
`asin_cache.csv` に ASIN ごとの取得状態を保存し、毎回の実行で差分更新キューを組み立てます。

キャッシュには以下を保持します（主要項目）:
- `asin`, `last_fetched_at`, `last_success_at`, `last_failure_at`
- `keepa_lastSoldUpdate`, `keepa_monthlySold`, `keepa_salesRankDrops30`
- `estimate_source`, `estimate_confidence`, `estimate_note`
- `failure_type`, `rows_seen_in_input`, `fetch_priority`, `next_fetch_after`, `consecutive_failures`

キュー選定の概要:
- `queue_decision=new`: キャッシュ未登録
- `queue_decision=retry`: 通信失敗・unavailable・主要データ欠損など
- `queue_decision=retry_not_found_due`: `keepa_product_not_found` の cooldown 経過後
- `queue_decision=skip_not_found_cooldown`: `keepa_product_not_found` で cooldown 中
- `queue_decision=stale`: `next_fetch_after` 到来 or 古いデータ
- `queue_decision=skip_cached`: キャッシュが新鮮で再取得不要

`next_fetch_after` の初期ルール:
- communication_error: 30分後
- keepa_product_not_found: 7日後（cooldown中は再試行しない）
- monthlySold あり: 7日後
- monthlySold なし / salesRankDrops30 あり: 3日後
- 両方欠損: 2日後

キャッシュを削除すると、次回実行時は全 ASIN が未取得扱いとなり、再フル取得できます。
キャッシュ保存は一時ファイル（`.tmp`）へ書き込み後 `os.replace()` で切替える atomic 方式です。

## keepa_lastSoldUpdate の形式
`keepa_lastSoldUpdate` は Excel 出力時に `YYYY-MM-DD HH:MM:SS` 形式の文字列です。

- 値が無い場合は空欄
- Keepa の minute 値や日時文字列を変換できる場合のみ整形（文字列日時が tz-aware の場合は Asia/Tokyo に変換後に整形）
- 変換失敗時は warning をログ出力し、元値（文字列）または空欄で継続

## input/output ファイル名
デフォルト値:
- 入力: `output.xlsx`
- 出力: `output_keepa.xlsx`
- ログ: `keepa_enrich.log`

ファイル名は `config.ini` で変更可能です。

## config.ini の書式
同梱の `config.ini.example` を `config.ini` にコピーして編集してください。

```ini
[keepa]
api_key =

[files]
input_excel = output.xlsx
output_excel = output_keepa.xlsx
asin_cache = asin_cache.csv

[app]
log_file = keepa_enrich.log
timeout_sec = 30

[run]
default_mode = single
reserve_tokens = 10
tokens_per_minute = 5
interval_seconds = 60
max_minutes = 480
```

APIキーの優先順:
1. `config.ini` の `[keepa] api_key`
2. 環境変数 `KEEPA_API_KEY`


## 実行モード（single / burst / drip）

### single
- 既存の1回実行モードです。
- 差分更新キューで今回取得対象になったASINを1回で処理して終了します。

### burst
- 起動時点の `available_tokens` を確認し、`reserve_tokens` を残した範囲で可能なだけ処理します。
- 予算式: `usable_tokens = max(0, available_tokens - reserve_tokens)`
- 1 ASIN = 1 token 前提で `usable_tokens` 件までを上限とします。
- 初回投入や手動で一気に進めたい運用向けです。

### drip
- 夜間の継続運用向けモードです。
- `interval_seconds` ごとに token status を見ながら安全な件数だけ進め、各サイクルでその場で実 fetch とキャッシュ更新を行います。
- 予算式: `target_tokens_this_cycle = tokens_per_minute * interval_seconds / 60`（整数化）
- `available_tokens - reserve_tokens` を超えるときは自動で減速します。
- トークン不足時は 0 件のループを許容し、待機とログを継続します。

### reserve_tokens の意味
- Keepaトークンを使い切らないための安全余力です。
- `available_tokens` が `reserve_tokens` 以下なら、その時点の取得件数は 0 になります。

### 途中停止条件
- `single`: 1回実行で終了
- `burst`: 予算消化またはキュー消化で終了
- `drip`: `max_minutes` 到達、`max_fetches` 到達、またはキュー消化で終了

## 実行方法

### Python で実行（開発時）
```bash
pip install -r requirements.txt
python keepa_enrich.py --mode single
python keepa_enrich.py --mode burst --reserve-tokens 10
python keepa_enrich.py --mode drip --tokens-per-minute 5 --interval-seconds 60 --max-minutes 480
python keepa_enrich.py --mode burst --dry-run
```

### 実行ファイル（配布先）
配布先では `KeepaMonthlySales.exe` と同じフォルダに
- `config.ini`
- `output.xlsx`
を置いて、用途に応じて以下を実行します。
- `run.bat`（single）
- `run_burst.bat`（burst）
- `run_drip.bat`（drip）

## ローカル Windows での PyInstaller ビルド例
> Codex 上では exe ビルドせず、以下をローカル Windows で実行してください。

```powershell
py -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
pyinstaller --noconfirm --clean --onedir --name KeepaMonthlySales keepa_enrich.py
```

生成物は `dist\KeepaMonthlySales\` に出力されます。

## 配布フォルダ例

```text
dist\KeepaMonthlySales\
  KeepaMonthlySales.exe
  run.bat
  config.ini         # config.ini.example をコピーして作成
  output.xlsx        # Amazon_Price_search の出力
```

Keepa product API への問い合わせは最大100 ASIN単位でまとめて送信し、返却productsに存在しないASINは `keepa_product_not_found` としてASIN単位で扱います。

## ログ分類
ログでは以下を区別します。
- `failure_type=communication_error`
  - HTTP エラー、タイムアウト、requests 例外、JSON decode 失敗など
- `failure_type=keepa_product_not_found`
  - Keepa から products が返らない / 対象 ASIN が products に存在しない
- `status=monthlySold_missing`
  - product はあるが monthlySold が null/NaN/0（<=0）
- `status=salesRankDrops30_missing`
  - product はあるが stats.salesRankDrops30 が null/NaN

## サマリ出力項目
標準出力とログに以下を出します。
- `total_input_rows`
- `rows_with_missing_asin`
- `rows_with_valid_asin`
- `unique_valid_asins`
- `queued_for_fetch_count`
- `skipped_by_cache_count`
- `fetched_success_count`
- `fetched_failure_count`
- `cache_hit_count`
- `cache_miss_count`
- `queue_priority_high_count`
- `queue_priority_medium_count`
- `queue_priority_low_count`
- `monthlySold_used_count`
- `salesRankDrops30_calibrated_count`
- `unavailable_count`
- `communication_error_count`
- `keepa_product_not_found_count`
- `monthlySold_missing_count`
- `salesRankDrops30_missing_count`
- `mode`
- `available_tokens_at_start`
- `reserve_tokens`
- `total_queue_count`
- `selected_fetch_count`
- `remaining_queue_count`
- `cycle_count`
- `total_sleep_seconds`
- `run_duration_seconds`
- `effective_tokens_per_minute`（drip）
- `max_minutes_reached`（drip）
- `queue_exhausted`（drip）
- `coefficient_value`

## よくある失敗と対処
- `Keepa APIキーが見つかりません`  
  - `config.ini` の `api_key` を設定するか、環境変数 `KEEPA_API_KEY` を設定してください。
- `入力ファイルが見つかりません`  
  - `output.xlsx` が exe/script と同じフォルダにあるか確認してください。
  - `config.ini` の `input_excel` 設定も確認してください。
- `入力ファイルに ASIN 列が存在しません`  
  - 入力 Excel に `ASIN` 列があるか確認してください。
- 一部 ASIN の取得失敗  
  - Keepa 側エラーや制限の可能性があります。処理は継続され、失敗内容はログ分類で確認できます。

## ログファイルの場所
ログファイルは `config.ini` の `log_file` で指定した名前で、実行ファイル（またはスクリプト）と同じフォルダに出力されます。
