# Keepa 月販推定付与ツール

## 機能概要
`output.xlsx` を読み込み、Keepa API から `monthlySold` / `stats.salesRankDrops30` を取得して、月販推定列を追加した `output_keepa.xlsx` を出力します。

主な仕様:
- **元の全行を保持**して出力（ASIN 空欄行も削除しない）
- API 呼び出し対象は **ASIN がある行のユニーク ASIN のみ**
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

## keepa_lastSoldUpdate の形式
`keepa_lastSoldUpdate` は Excel 出力時に `YYYY-MM-DD HH:MM:SS` 形式の文字列です。

- 値が無い場合は空欄
- Keepa の minute 値や日時文字列を変換できる場合のみ整形
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
- `monthlySold_used_count`
- `salesRankDrops30_calibrated_count`
- `unavailable_count`
- `communication_error_count`
- `keepa_product_not_found_count`
- `monthlySold_missing_count`
- `salesRankDrops30_missing_count`
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
