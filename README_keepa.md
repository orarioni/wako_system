# Keepa 月販推定付与ツール

## 機能概要
`output.xlsx` を読み込み、Keepa API から `monthlySold` / `stats.salesRankDrops30` を取得して、月販推定列を追加した `output_keepa.xlsx` を出力します。

主な仕様:
- ASIN 空欄行は Keepa API 問い合わせ対象外（出力には行を保持）
- `monthlySold` を最優先で推定値に採用
- `monthlySold` が無い場合は `salesRankDrops30 × 補正係数` で推定
- 補正係数は `monthlySold >= 1` かつ `salesRankDrops30 >= 1` の `monthlySold / salesRankDrops30` の中央値
- 係数が算出できない場合は `1.0`
- 一部 ASIN で API 失敗があっても全体処理は継続

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

[app]
log_file = keepa_enrich.log
timeout_sec = 30
```

APIキーの優先順:
1. `config.ini` の `[keepa] api_key`
2. 環境変数 `KEEPA_API_KEY`

## 実行方法

### Python で実行（開発時）
```bash
pip install -r requirements.txt
python keepa_enrich.py
```

### 実行ファイル（配布先）
配布先では `KeepaMonthlySales.exe` と同じフォルダに
- `config.ini`
- `output.xlsx`
を置いて、`run.bat` を実行します。

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

## よくある失敗と対処
- `Keepa APIキーが見つかりません`  
  - `config.ini` の `api_key` を設定するか、環境変数 `KEEPA_API_KEY` を設定してください。
- `入力ファイルが見つかりません`  
  - `output.xlsx` が exe/script と同じフォルダにあるか確認してください。
  - `config.ini` の `input_excel` 設定も確認してください。
- `入力ファイルに ASIN 列が存在しません`  
  - 入力 Excel に `ASIN` 列があるか確認してください。
- 一部 ASIN の取得失敗  
  - Keepa 側エラーや制限の可能性があります。処理は継続され、失敗 ASIN はログに出ます。

## ログファイルの場所
ログファイルは `config.ini` の `log_file` で指定した名前で、実行ファイル（またはスクリプト）と同じフォルダに出力されます。
