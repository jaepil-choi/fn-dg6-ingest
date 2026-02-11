# fn-dg6-ingest

FnGuide DataGuide 6에서 내보낸 CSV/Excel 파일을 자동 감지하고, 정제된 관계형 테이블(CSV/Parquet)로 변환하는 Python 라이브러리.

---

## Quick Start

### 1. 설치

```bash
# uv 사용 (권장)
uv sync

# 또는 pip
pip install -e .
```

### 2. 데이터 준비

DataGuide 6에서 내보낸 파일을 프로젝트 루트의 `inputs/` 폴더에 넣으세요.

```
fn-dg6-ingest/
  inputs/                          # ← 여기에 DG6 파일을 넣으세요
    dataguide_kse+kosdaq_ohlcv_from(20160101)_to(20260207).csv
    dataguide_etfconst(kodex200)_from(20250101)_to(20260207).csv
    ...
```

**지원 포맷:**

| 포맷 | 예시 | 설명 |
|------|------|------|
| Time Series Wide | OHLCV, 매출 컨센서스 | 날짜가 열(column)로 펼쳐진 형태 |
| Misc (ETF 구성종목 등) | ETF 구성종목 | 이미 관계형(long) 형태인 데이터 |

### 3. 실행

```python
from fn_dg6_ingest import init, ingest

# ── 최초 실행: 설정 파일 생성 + 데이터 변환 ──
config_path = init(
    input_path="inputs/dataguide_kse+kosdaq_ohlcv_from(20160101)_to(20260207).csv",
    output_dir="outputs/",
)
# → fnconfig.yaml 생성 + outputs/ 에 Parquet 파일 출력

# ── 이후 실행: 설정 파일 기반으로 재빌드 ──
written_files = ingest(config_path="fnconfig.yaml")
```

`init()`을 호출하면 두 가지가 일어납니다:

1. **`fnconfig.yaml` 자동 생성** -- 감지된 포맷, 메타데이터, 아이템 목록이 기록됩니다.
2. **데이터 변환 및 출력** -- 정제된 테이블이 `outputs/`에 저장됩니다.

설정 파일만 먼저 만들고 싶으면 `run_immediately=False`를 지정하세요:

```python
config_path = init(
    input_path="inputs/your_file.csv",
    output_dir="outputs/",
    run_immediately=False,  # 설정 파일만 생성, 데이터 변환은 나중에
)
```

### 4. 설정 편집 (선택)

자동 생성된 `fnconfig.yaml`을 편집하여 테이블 분할, 출력 포맷 등을 조정할 수 있습니다.

```yaml
# fnconfig.yaml (자동 생성 예시)
source:
  input_path: "inputs/dataguide_kse+kosdaq_ohlcv_from(20160101)_to(20260207).csv"
  detected_format: timeseries_wide

metadata:
  frequency: 일간
  currency: 원화
  period_start: "20160101"
  period_end: "최근일자(20260206)"

output:
  output_dir: "outputs/"
  output_format: parquet        # csv 또는 parquet
  normalize_units: true         # (천원)→(원) 등 단위 자동 환산
  drop_empty_entities: true     # 전체 NaN인 종목 제거

tables:
  # 기본값: 모든 아이템을 하나의 테이블로 출력
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

편집 후 `ingest()`를 다시 실행하면 변경된 설정이 반영됩니다:

```python
ingest(config_path="fnconfig.yaml")
```

### 5. 출력 결과

`outputs/` 폴더에 다음 파일이 생성됩니다:

```
outputs/
  default.parquet       # 데이터 테이블 (tables 그룹명 기준)
  _meta.parquet         # 데이터 리니지 (출처, 단위 환산 이력, 처리 시각 등)
```

테이블을 분할한 경우:

```
outputs/
  ohlcv.parquet         # 시가/고가/저가/종가
  volume.parquet        # 거래량/거래대금
  _meta.parquet
```

**`_meta` 테이블**은 각 아이템의 출처, 원본 단위, 적용된 환산 배수, 제거된 종목 수 등 처리 이력을 기록합니다.

---

## 주요 기능

- **포맷 자동 감지** -- 레이아웃 YAML 기반 좌표 탐지로 Time Series / Misc 포맷을 자동 판별
- **단위 자동 환산** -- `(천원)`, `(억원)`, `(십억원)` 등의 금액 단위를 `(원)` 기준으로 환산
- **빈 종목 제거** -- 전 기간 데이터가 없는 종목을 자동 제외
- **Config-First 워크플로우** -- `fnconfig.yaml` 하나로 테이블 분할, 출력 포맷, 정제 옵션을 제어
- **데이터 리니지** -- `_meta` 테이블로 처리 이력을 추적

---

## 프로그래밍 방식 설정 변경

YAML을 직접 편집하는 대신 Python에서 설정을 수정할 수도 있습니다:

```python
from fn_dg6_ingest.config import load_config, save_config

cfg = load_config("fnconfig.yaml")
cfg.tables = {
    "ohlcv": ["수정시가(원)", "수정고가(원)", "수정저가(원)", "수정주가(원)"],
    "volume": ["거래량(주)", "거래대금(원)"],
}
cfg.output.output_format = "csv"
save_config(cfg, "fnconfig.yaml")
```

---

## 테스트

```bash
# 단위 테스트만
uv run python -m pytest tests/unit/ -v

# 통합 테스트 (inputs/ 에 실제 파일 필요)
uv run python -m pytest tests/integration/ -v -m integration

# 전체
uv run python -m pytest -v
```

---

## 요구 사항

- Python >= 3.14
- pandas, pyarrow, pydantic, pyyaml, openpyxl
