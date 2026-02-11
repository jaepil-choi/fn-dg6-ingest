# fn-dg6-ingest

FnGuide DataGuide 6에서 내보낸 CSV/Excel 파일을 자동 감지하고, 정제된 관계형 테이블(CSV/Parquet)로 변환하며, 변환된 데이터를 프로그래밍 방식으로 읽을 수 있게 해주는 Python 라이브러리.

## Why?

2025년 12월, FnGuide DataGuide가 버전 5에서 6으로 업데이트되었습니다. 기존에 DataGuide에서 엑셀로 데이터를 내보낸 뒤 Python에서 활용하는 과정에는 여러 불편함이 있었습니다:

- **날짜가 컬럼에 펼쳐지는 구조** -- 대량 데이터 내보내기 시 날짜가 열(column)로 나열되어 처리가 번거로움
- **비관계형 레이아웃** -- 일반적인 관계형 DB(long format)와 다른 구조라 분석에 바로 쓰기 어려움
- **빈 종목 데이터** -- 상장 폐지 등으로 데이터가 없는 종목도 전체 기간에 걸쳐 null 행이 존재
- **느린 로딩 속도** -- 큰 파일을 그대로 읽으면 불필요한 데이터까지 스캔

DataGuide 6 업데이트를 계기로, **내보낸 파일만 넣으면 정제된 CSV/Parquet DB로 자동 변환해 주는 라이브러리**를 만들고자 이 프로젝트를 시작했습니다.

---

## Quick Start

### 1. 설치

```bash
# 아직 PyPI에 배포되지 않았으므로 저장소를 클론하여 설치합니다.
git clone https://github.com/youruser/fn-dg6-ingest.git
cd fn-dg6-ingest

# uv 사용 (권장)
uv pip install -e .

# 또는 pip
pip install -e .
```

### 2. 데이터 준비

DataGuide 6에서 내보낸 파일을 프로젝트 루트의 `inputs/` 폴더에 넣으세요.

```
fn-dg6-ingest/
  inputs/                          # ← 여기에 DG6 파일을 넣으세요
    dataguide_kse+kosdaq_ohlcv_from(20160101)_to(20260207).csv
    dataguide_kse+kosdaq_sales-consensus_from(20180101)_to(20260207).csv
    dataguide_etfconst(kodex200)_from(20250101)_to(20260207).csv
```

**지원 포맷:**

| 포맷 | 예시 | 설명 |
|------|------|------|
| Time Series Wide | OHLCV, 매출 컨센서스 | 날짜가 열(column)로 펼쳐진 형태 |
| Misc (ETF 구성종목 등) | ETF 구성종목 | 이미 관계형(long) 형태인 데이터 |

### 3. 실행

```python
import fn_dg6_ingest

# ── open(): 단일 진입점 ──
# 최초 실행: 포맷 감지 → 설정 생성 → 데이터 변환 → Dataset 반환
ds = fn_dg6_ingest.open(
    "inputs/dataguide_kse+kosdaq_ohlcv_from(20160101)_to(20260207).csv",
    output_dir="outputs/kse+kosdaq_ohlcv",
)

# 두 번째 호출: 이미 데이터가 존재하면 변환을 건너뛰고 바로 Dataset 반환 (멱등성)
ds = fn_dg6_ingest.open(
    "inputs/dataguide_kse+kosdaq_ohlcv_from(20160101)_to(20260207).csv",
    output_dir="outputs/kse+kosdaq_ohlcv",
)

# ── 데이터 읽기 ──
df = ds.load()                                              # 전체 로드
df = ds.load(codes=["A005930"], items=["수정주가(원)"])       # 삼성전자 종가만
df = ds.load(date_from="2024-01-01", date_to="2025-12-31")  # 날짜 범위

# ── 메타데이터 조회 ──
meta = ds.load_meta()    # _meta 메타 테이블
info = ds.describe()     # 빠른 메타데이터 (데이터 스캔 없이)
```

또는 데모 스크립트로 전체 입력 파일을 일괄 처리:

```bash
uv run python scripts/run_ingest.py            # 멱등: 데이터 있으면 건너뜀
uv run python scripts/run_ingest.py --force     # 강제 재빌드
```

`open()`은 **입력에 따라 다형적으로 동작**합니다:

| 인자 유형 | 동작 |
|-----------|------|
| DG6 원본 파일 (CSV/Excel) | 최초 실행: 포맷 감지 → 설정 생성 → 출력 빌드. `Dataset` 반환. 이미 출력이 존재하면 빌드 건너뜀. |
| 기존 YAML 설정 파일 | 설정 로드 → `Dataset` 반환. 파이프라인 실행 없음. |

```python
# 설정 파일에서 직접 열기
ds = fn_dg6_ingest.open("outputs/kse+kosdaq_ohlcv.yaml")
df = ds.load()
```

설정 파일만 먼저 만들고 싶으면 `run_immediately=False`를 지정하세요:

```python
ds = fn_dg6_ingest.open(
    "inputs/your_file.csv",
    output_dir="outputs/",
    run_immediately=False,  # 설정 파일만 생성, 데이터 변환은 나중에
)
ds.ingest()  # 필요할 때 수동 빌드
```

### 4. 설정 편집 및 재빌드

자동 생성된 `fnconfig.yaml`을 편집하여 테이블 분할, 출력 포맷 등을 조정할 수 있습니다.

```yaml
# outputs/kse+kosdaq_ohlcv.yaml (자동 생성된 설정)
source:
  input_path: inputs/dataguide_kse+kosdaq_ohlcv_from(20160101)_to(20260207).csv
  detected_format: timeseries_wide

metadata:
  frequency: 일간
  currency: 원화
  sort_order: 오름차순
  non_business_days: 제외
  include_weekends: 제외
  period_start: '20160101'
  period_end: 최근일자(20260206)
  calendar_basis: true

output:
  output_dir: outputs/kse+kosdaq_ohlcv
  output_format: parquet        # csv 또는 parquet
  normalize_units: true         # (천원)→(원) 등 단위 자동 환산
  drop_empty_entities: true     # 전체 NaN인 종목 제거

tables:
  default:
    - 수정시가(원)
    - 수정고가(원)
    - 수정저가(원)
    - 수정주가(원)
    - 거래량(주)
    - 거래대금(원)

  # 여러 테이블로 분할하려면 그룹을 추가하세요:
  # ohlcv:
  #   - 수정시가(원)
  #   - 수정고가(원)
  #   - 수정저가(원)
  #   - 수정주가(원)
  # volume:
  #   - 거래량(주)
  #   - 거래대금(원)
```

편집 후 Dataset을 통해 재빌드합니다:

```python
ds = fn_dg6_ingest.open("outputs/kse+kosdaq_ohlcv.yaml")
ds.ingest()  # 변경된 설정으로 재빌드

# 분할된 테이블 로드
df = ds.load(table="ohlcv")
```

또는 Python에서 프로그래밍 방식으로 설정을 수정할 수도 있습니다:

```python
ds = fn_dg6_ingest.open("outputs/kse+kosdaq_ohlcv.yaml")

ds.config.tables = {
    "ohlcv": ["수정시가(원)", "수정고가(원)", "수정저가(원)", "수정주가(원)"],
    "volume": ["거래량(주)", "거래대금(원)"],
}
ds.config.output.output_format = "csv"

ds.save_config()  # YAML에 반영
ds.ingest()       # 재빌드 → ohlcv.csv, volume.csv, _meta.csv
```

---

## Dataset API

### `Dataset` 객체

`fn_dg6_ingest.open()`이 반환하는 핸들 객체. 경로를 한 번만 지정하면 이후 모든 작업은 이 핸들을 통해 수행합니다.

```python
ds = fn_dg6_ingest.open("outputs/kse+kosdaq_ohlcv.yaml")

ds.config          # IngestConfig (파싱된 설정)
ds.config_path     # YAML 파일 경로
ds.output_dir      # 출력 디렉토리 경로
```

### `ds.load()` -- 필터링을 지원하는 데이터 읽기

Parquet의 **열 선택(column pruning)**과 **술어 푸시다운(predicate pushdown)**을 활용하여 대용량 데이터에서 필요한 부분만 효율적으로 읽습니다.

```python
df = ds.load()                                          # 전체 데이터
df = ds.load(table="ohlcv")                             # 특정 테이블
df = ds.load(codes=["A005930"])                         # 종목 코드로 필터
df = ds.load(date_from="2024-01-01", date_to="2025-12-31")  # 날짜 범위
df = ds.load(items=["수정주가(원)", "거래량(주)"])         # 아이템(컬럼) 선택
df = ds.load(codes=["A005930"], date_from="2024-01-01",
             items=["수정주가(원)"])                      # 조합
```

테이블이 1개면 `DataFrame`, 여러 개면 `dict[str, DataFrame]`을 반환합니다.

### `ds.load_meta()` -- 메타 테이블 읽기

```python
meta = ds.load_meta()  # _meta DataFrame (20개 컬럼)
```

### `ds.describe()` -- 빠른 메타데이터 조회

Parquet 파일 푸터에서 스키마와 행 수를 읽어 **데이터를 스캔하지 않고** 빠르게 정보를 제공합니다.

```python
info = ds.describe()
info.tables         # ["default"] 또는 ["ohlcv", "volume"]
info.items          # {"default": ["수정시가(원)", ...]}
info.shape          # {"default": (7613009, 9)}
info.date_range     # ("20160101", "최근일자(20260206)")
info.entities       # 3071
info.format_name    # "timeseries_wide"
```

### `ds.ingest()` -- 재빌드

```python
ds.ingest()        # 현재 설정으로 출력 재생성
```

### `ds.save_config()` -- 설정 저장

```python
ds.config.tables = {"ohlcv": [...], "volume": [...]}
ds.save_config()   # YAML에 반영
```

---

## 실제 출력 예시

세 가지 DataGuide 6 파일에 대해 `scripts/run_ingest.py`를 실행한 결과입니다.

### KSE+KOSDAQ OHLCV (Time Series Wide)

| 항목 | 값 |
|------|-----|
| 감지 포맷 | `timeseries_wide` |
| 출력 파일 | `default.parquet` (113.8 MB) |
| 행 x 열 | 7,613,009 x 9 |
| 메모리 | 786.3 MB |
| 기간 | 2016-01-01 ~ 2026-02-06 (일간) |
| 종목 | 4,071개 중 1,000개 빈 종목 제거 → 3,071개 |

```
     코드  코드명       date  수정시가(원)  수정고가(원)  수정저가(원)  수정주가(원)    거래량(주)      거래대금(원)
  A000020 동화약품 2015-12-30   8180.0   8180.0   8020.0   8140.0  166761.0 1.348911e+09
  A000020 동화약품 2016-01-04   8130.0   8150.0   7920.0   8140.0  281440.0 2.265829e+09
  A000020 동화약품 2016-01-05   8040.0   8250.0   8000.0   8190.0  243179.0 1.981977e+09
  A000020 동화약품 2016-01-06   8200.0   8590.0   8110.0   8550.0  609906.0 5.129946e+09
  A000020 동화약품 2016-01-07   8470.0   8690.0   8190.0   8380.0  704752.0 5.919556e+09
```

### KSE+KOSDAQ Sales Consensus (Time Series Wide, 단위 환산)

| 항목 | 값 |
|------|-----|
| 감지 포맷 | `timeseries_wide` |
| 출력 파일 | `default.parquet` (13.0 MB) |
| 행 x 열 | 4,913,310 x 13 |
| 메모리 | 661.4 MB |
| 기간 | 2018-01-01 ~ 2026-02-06 (일간) |
| 종목 | 4,071개 중 1,602개 빈 종목 제거 → 2,469개 |
| 단위 환산 | `매출액(억원)` → `매출액(원)` (x100,000,000) 등 7개 아이템 |

단위 환산이 적용된 `_meta` 행 예시:

| 아이템명 | 아이템명_normalized | unit_original | unit_multiplier |
|----------|---------------------|---------------|-----------------|
| 매출액(억원) | 매출액(원) | 억원 | 100,000,000 |
| 매출액(Fwd.12M)(억원) | 매출액(Fwd.12M)(원) | 억원 | 100,000,000 |
| 추정기관수 | 추정기관수 | NaN | 1 |

### ETF 구성종목 -- KODEX 200 (Misc)

| 항목 | 값 |
|------|-----|
| 감지 포맷 | `misc_etf` |
| 출력 파일 | `default.parquet` (380.4 KB) |
| 행 x 열 | 53,836 x 8 |
| 메모리 | 5.9 MB |
| 기간 | 2025-01-01 ~ 2026-02-06 (일간) |

```
       date   ETF코드      ETF명  구성종목코드      구성종목  주식수(계약수)       금액  금액기준 구성비중(%)
 2025-01-02 A069500 KODEX 200              원화현금       NaN   9945243          0.62
 2025-01-02 A069500 KODEX 200 A000080     하이트진로      47.0    914620          0.06
 2025-01-02 A069500 KODEX 200 A000100      유한양행      91.0  10765300          0.67
 2025-01-02 A069500 KODEX 200 A000120    CJ대한통운      17.0   1429700          0.09
 2025-01-02 A069500 KODEX 200 A000660    SK하이닉스     851.0 145691200          9.09
```

### 출력 디렉토리 구조

```
outputs/
├── etfconst(kodex200).yaml              # 설정 파일
├── etfconst(kodex200)/
│   ├── default.parquet                  # 데이터 테이블
│   └── _meta.parquet                    # 메타 테이블
├── kse+kosdaq_ohlcv.yaml
├── kse+kosdaq_ohlcv/
│   ├── default.parquet
│   └── _meta.parquet
├── kse+kosdaq_sales-consensus.yaml
└── kse+kosdaq_sales-consensus/
    ├── default.parquet
    └── _meta.parquet
```

### `_meta` 테이블 스키마 (20개 컬럼)

모든 데이터셋에 `_meta.parquet`가 함께 출력됩니다. 각 아이템의 출처, 단위 환산 이력, 제거된 종목 수 등을 기록합니다.

| Column | 예시 | 설명 |
|--------|------|------|
| `table_name` | `default` | 소속 출력 테이블명 |
| `source_file` | `dataguide_kse+kosdaq_ohlcv_...csv` | 원본 파일명 |
| `source_hash` | `523e45d8...` | 원본 파일 SHA-256 |
| `source_last_updated` | `2026-02-07 15:46:56` | DG6 Refresh 시각 |
| `detected_format` | `timeseries_wide` | 감지된 레이아웃 |
| `아이템코드` | `S410000650` | DG6 아이템 코드 |
| `아이템명` | `수정시가(원)` | 원본 아이템명 |
| `아이템명_normalized` | `수정시가(원)` | 단위 환산 후 이름 |
| `유형` | `SSC` | 종목 유형 |
| `집계주기` | `일간` | 집계 주기 |
| `unit_original` | `억원` | 원본 단위 접미사 |
| `unit_multiplier` | `100000000` | 적용된 환산 배수 |
| `entities_total` | `4071` | 원본 종목 수 |
| `entities_dropped` | `1000` | 제거된 빈 종목 수 |
| `processed_at` | `2026-02-11T02:15:02+00:00` | 처리 시각 (UTC) |

---

## 주요 기능

- **Dataset 핸들** -- `open()` 한 번으로 경로를 기억하고, `load()` / `describe()` / `ingest()`를 통합 제공
- **멱등 open()** -- 출력 데이터가 이미 존재하면 변환을 건너뛰고 즉시 `Dataset` 반환 (`force=True`로 강제 재빌드 가능)
- **Parquet 네이티브 필터링** -- 열 선택(column pruning) + 술어 푸시다운(predicate pushdown)으로 대용량 데이터 효율 읽기
- **포맷 자동 감지** -- 레이아웃 YAML 기반 좌표 탐지로 Time Series / Misc 포맷을 자동 판별
- **단위 자동 환산** -- `(천원)`, `(억원)`, `(십억원)` 등의 금액 단위를 `(원)` 기준으로 환산
- **빈 종목 제거** -- 전 기간 데이터가 없는 종목을 자동 제외 (OHLCV: 4,071개 중 1,000개 제거)
- **Config-First 워크플로우** -- `fnconfig.yaml` 하나로 테이블 분할, 출력 포맷, 정제 옵션을 제어
- **메타 테이블** -- `_meta` 테이블로 처리 이력 추적 (원본 해시, 단위 환산 내역, 종목 통계)

---

## 스크립트

| 스크립트 | 설명 |
|----------|------|
| `scripts/run_ingest.py` | `inputs/`의 전체 파일을 일괄 처리 (멱등, `--force`로 강제 재빌드) |
| `scripts/inspect_outputs.py` | Dataset API를 사용하여 `outputs/`의 결과물 속성 및 샘플 출력 |

```bash
# 전체 파일 일괄 처리 (데이터 있으면 건너뜀)
uv run python scripts/run_ingest.py

# 강제 재빌드
uv run python scripts/run_ingest.py --force

# 출력 결과 전체 점검
uv run python scripts/inspect_outputs.py

# 특정 데이터셋만 점검 (YAML 설정 경로로 지정)
uv run python scripts/inspect_outputs.py outputs/kse+kosdaq_ohlcv.yaml
```

---

## 테스트

```bash
# 단위 테스트 (195개, ~3초)
uv run python -m pytest tests/unit/ -v

# 통합 테스트 (31개, inputs/ 에 실제 파일 필요, ~42분)
uv run python -m pytest tests/integration/ -v -m integration

# 전체
uv run python -m pytest -v
```

---

## 요구 사항

- Python >= 3.14
- pandas, pyarrow, pydantic, pyyaml, openpyxl
