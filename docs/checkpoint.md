# 체크포인트 -- 2026-02-11

## 현재 상태

**브랜치**: `develop` (168개 테스트 통과; Phase 1 완료)

**Git 이력** (최신순):

```
* d1a93c4 (develop) Add demo scripts, fix build config, update README with real output data
*   0937ed9 Merge feat/api: public API (init + ingest) with 18 integration tests
|\
| * da2d154 Implement public API (init + ingest) with 18 integration tests
|/
*   481687f Merge feat/meta-export: meta table builder + exporter with 37 tests
|\
| * 4440e70 Update checkpoint: meta-export done, 150 tests, detailed API plan next
| * d184697 Implement meta table builder and exporter with 37 unit tests
|/
* 14896da Update checkpoint: transforms done, 113 tests, next is meta-export
*   bce527b Merge feat/transforms into develop
|\
| * b454428 Implement transform pipeline: numbers, units, empty drop, splitter
|/
*   211762d Merge fix/parser-architecture into develop
*   91a0431 Merge feat/config: Pydantic models, YAML I/O, 35 unit tests
*   d00a46f Merge feat/scaffold: PRD, package layout, test structure
*   44866ad Install dependencies
*   ce25106 Init uv project
*   9680ea6 Initial commit
```

---

## 구축 완료된 모듈

### Phase 1 -- 핵심 (MVP) 전체 완료

| 모듈 | 파일 | 상태 |
|--------|------|--------|
| 공개 API | `__init__.py` | **완료** -- `init()`, `ingest()` 구현 |
| 설정 | `config.py` | **완료** -- 모델, YAML I/O, 검증 |
| 감지 | `detect.py` | **완료** -- 레이아웃 기반 좌표 감지 |
| 레이아웃 로더 | `layout_registry.py` | **완료** -- Pydantic 모델, YAML 로더, `extract_settings()` |
| 레이아웃 정의 | `layouts/*.yaml` | **완료** -- `timeseries_wide`, `misc_etf` |
| 파서 베이스 | `parsers/base.py` | **완료** -- ABC, `ParseResult`(`key_columns` 포함), `ItemInfo` |
| TS Wide 파서 | `parsers/timeseries.py` | **완료** -- melt+pivot, 메타데이터 추출 |
| Misc 파서 | `parsers/misc.py` | **완료** -- 숫자 컬럼 감지, 메타데이터 |
| Snapshot 파서 | `parsers/snapshot.py` | 스텁 (샘플 데이터 없음) |
| 숫자 파싱 | `transforms/numbers.py` | **완료** -- 쉼표/공백 제거, 숫자 변환 |
| 단위 정규화 | `transforms/units.py` | **완료** -- 접미사 감지, 스케일링, (원)으로 리네이밍 |
| 빈 종목 제거 | `transforms/empty.py` | **완료** -- 전체 NaN 종목 제거, 카운트 리포트 |
| 테이블 분할 | `transforms/splitter.py` | **완료** -- 아이템명 그룹별 분할, 키 컬럼 보존 |
| 파이프라인 | `transforms/pipeline.py` | **완료** -- 오케스트레이터, `PipelineResult`, 설정 기반 |
| 메타 테이블 | `meta.py` | **완료** -- `build_meta_table()`, `_compute_file_hash()` |
| Exporter | `export.py` | **완료** -- CSV + Parquet, 디렉토리 생성 |
| 예외 | `exceptions.py` | **완료** |

**테스트**: 168개 통과 (35 config + 12 detect + 66 transforms + 20 meta + 13 export + 4 helpers + 18 integration)

**실제 데이터 검증 완료**:
- OHLCV: 7,613,009행 x 9열, 113.8 MB Parquet, 4,071 종목 중 1,000개 빈 종목 제거
- 매출 컨센서스: 4,913,310행 x 13열, 13.0 MB Parquet, 억원→원 단위 환산 검증
- ETF 구성종목: 53,836행 x 8열, 380.4 KB Parquet, misc_etf 포맷 감지 검증

---

## 다음 할 일: Phase 2 -- Dataset 인터페이스

### 아키텍처 변경의 핵심 동기

현재 라이브러리는 **쓰기 전용(write-only)**이다. `init()`과 `ingest()`로 파일을 생성하지만, 생성된 데이터를 읽으려면 사용자가 직접 경로를 구성하고 `pd.read_parquet()`을 호출해야 한다. 또한 매 함수 호출마다 `config_path`를 반복 지정해야 한다.

**Dataset 핸들 패턴**으로 이 문제를 해결한다:
- 경로를 한 번만 지정하면 핸들 객체가 기억
- 쓰기(`ingest`)와 읽기(`load`)를 단일 객체에 통합
- Parquet 네이티브 필터링으로 대용량 데이터 효율적 접근

### 0. 브랜치 생성

```
git checkout develop
git checkout -b feat/dataset
```

### 1. `reader.py` -- 데이터 읽기 로직 (신규 파일)

Parquet/CSV 읽기 로직을 별도 모듈로 분리한다. `Dataset`이 이 모듈을 호출한다.

**`read_table(output_dir, table_name, output_format, ...) -> DataFrame`**:
- `output_format`에 따라 Parquet 또는 CSV 읽기
- Parquet: `pyarrow.parquet.read_table()`으로 필터링 지원
  - `columns` 파라미터로 **열 선택(column pruning)**: 키 컬럼 + 지정된 items만 로드
  - `filters` 파라미터로 **행 필터링(predicate pushdown)**: codes, date 범위
  - 주의: `date` 컬럼이 현재 `str` 타입이므로, 필터링 전 비교 형식을 맞춰야 함
- CSV: `pd.read_csv()` 후 pandas에서 post-load 필터링 (동일 인터페이스, 성능만 다름)
- key_columns 정보는 config에서 직접 추론하거나 _meta에서 읽을 수 있음

**`read_meta(output_dir, output_format) -> DataFrame`**:
- `_meta.{format}` 파일 읽기

**`read_table_info(output_dir, table_name, output_format) -> TableInfo`**:
- Parquet: `pyarrow.parquet.read_metadata()`로 행 수, 스키마를 **데이터 스캔 없이** 읽기
- CSV: `pd.read_csv(nrows=0)`으로 스키마만, 행 수는 _meta에서 추론

단위 테스트 (`test_reader.py`):
- Parquet 열 선택 테스트 (지정된 items만 로드되는지)
- Parquet 행 필터링 테스트 (codes, date 범위)
- CSV 폴백 테스트 (동일 인터페이스)
- 존재하지 않는 파일 에러 처리
- 빈 필터 (전체 로드) 테스트

### 2. `dataset.py` -- Dataset 클래스 (신규 파일)

**`Dataset` 클래스 설계**:

```python
class Dataset:
    def __init__(self, config: IngestConfig, config_path: Path):
        self.config = config
        self.config_path = config_path

    @property
    def output_dir(self) -> Path:
        return Path(self.config.output.output_dir)

    def ingest(self) -> list[str]:
        """설정 기반 재빌드. 내부적으로 기존 파이프라인 로직 호출."""

    def load(
        self,
        table: str | None = None,
        codes: list[str] | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        items: list[str] | None = None,
    ) -> pd.DataFrame | dict[str, pd.DataFrame]:
        """출력 데이터 로드. table 지정 시 단일 DataFrame, 미지정 시 dict."""

    def load_meta(self) -> pd.DataFrame:
        """_meta 리니지 테이블 로드."""

    def describe(self) -> DatasetInfo:
        """데이터 스캔 없이 빠른 메타데이터 조회."""

    def save_config(self) -> None:
        """현재 self.config를 YAML로 저장."""
```

**`DatasetInfo` dataclass**:

```python
@dataclass
class DatasetInfo:
    config_path: str
    format_name: str
    tables: list[str]            # 테이블명 목록
    items: dict[str, list[str]]  # 테이블명 -> 아이템 목록
    shape: dict[str, tuple[int, int]]  # 테이블명 -> (rows, cols)
    date_range: tuple[str, str] | None  # (시작, 종료) from _meta
    entities: int | None         # 종목 수 (from _meta)
    output_format: str
    output_dir: str
```

**핵심 결정사항**:
- `Dataset.ingest()`는 기존 `__init__.py`의 `_run_pipeline_and_export()` 로직을 재활용. 파이프라인 실행 후 `self`를 갱신하지는 않음 (설정이 바뀌면 사용자가 `save_config()` 후 `ingest()` 호출).
- `load(table=None)`일 때 테이블이 1개면 DataFrame 반환, 여러 개면 dict 반환. 이 동작이 가장 직관적.
- `describe()`는 가능하면 Parquet 메타데이터에서, 불가능하면 `_meta` 테이블에서 정보를 수집.

단위 테스트 (`test_dataset.py`):
- Dataset 생성 (config + path)
- `load()` 기본 동작 (전체 로드)
- `load(table=...)` 단일 테이블 로드
- `load(codes=...)` 종목 필터링
- `load(date_from=..., date_to=...)` 날짜 필터링
- `load(items=...)` 아이템 선택
- `load_meta()` 메타 테이블 로드
- `describe()` 반환값 구조 검증
- 존재하지 않는 테이블명 에러
- `save_config()` 왕복 테스트

### 3. `__init__.py` 변경 -- `open()` 진입점 + 반환 타입 변경

**`open(path, output_dir?, config_path?, run_immediately?) -> Dataset`**:

```python
def open(
    path: str,
    output_dir: str | None = None,
    config_path: str | None = None,
    run_immediately: bool = True,
) -> Dataset:
```

로직:
1. `path`가 `.yaml` 파일인지 확인
   - **예**: `load_config(path)` → `Dataset(config, path)` 반환
   - **아니오**: DG6 원본 파일로 간주
     - `output_dir` 미지정 시 `path`에서 파생 (예: `inputs/dataguide_xxx.csv` → `outputs/xxx/`)
     - `config_path` 미지정 시 `output_dir` 옆에 `{name}.yaml`로 파생
     - 기존 `init()` 로직 실행 (감지 → 파싱 → 설정 생성 → 선택적 빌드)
     - `Dataset(config, config_path)` 반환

**`init()` 변경**: 반환 타입을 `str` → `Dataset`으로 변경.
**`ingest()` 변경**: 반환 타입을 `list[str]` → `Dataset`으로 변경. 기존 로직은 `Dataset.ingest()` 내부로 이동하고, 모듈 수준 `ingest()`는 `open(config_path).ingest()`의 래퍼.

**기존 코드와의 호환성**:
- `init()`이 `Dataset`을 반환하므로 `str`처럼 쓰던 코드는 `ds.config_path`로 대체 필요
- 기존 통합 테스트에서 `init()` 반환값을 `str`로 사용하는 부분 업데이트 필요

### 4. 기존 통합 테스트 업데이트

`tests/integration/test_timeseries.py`, `test_misc_etf.py`, `test_config_roundtrip.py`, `test_export.py`에서:
- `init()` 반환값을 `Dataset` 객체로 받도록 변경
- `config_path` 접근은 `ds.config_path` 사용
- 가능한 곳에서 `ds.load()`, `ds.load_meta()` 사용하도록 리팩토링

### 5. 신규 통합 테스트 (`test_dataset_e2e.py`)

**`tests/integration/test_dataset_e2e.py`**:

OHLCV 데이터 기반:
- `test_open_source_file`: `open(source_csv)` → Dataset 반환, 설정 생성됨, 출력 파일 존재
- `test_open_config_yaml`: `open(config_yaml)` → Dataset 반환, `load()` 가능
- `test_load_full`: `ds.load()` → 전체 DataFrame, shape 검증
- `test_load_single_table`: 설정 분할 후 `ds.load(table="ohlcv")` → 해당 테이블만
- `test_load_filter_codes`: `ds.load(codes=["A005930"])` → 삼성전자만
- `test_load_filter_date_range`: `ds.load(date_from="2025-01-01")` → 날짜 필터
- `test_load_filter_items`: `ds.load(items=["수정주가(원)"])` → 컬럼 선택
- `test_load_combined_filters`: codes + date + items 조합
- `test_load_meta`: `ds.load_meta()` → 20개 컬럼 스키마 검증
- `test_describe`: `ds.describe()` → DatasetInfo 필드 검증
- `test_ingest_rebuild`: `ds.config` 변경 → `ds.save_config()` → `ds.ingest()` → 재빌드 확인

ETF 데이터 기반:
- `test_open_etf_source`: misc 포맷에서도 `open()` + `load()` 동작
- `test_etf_describe`: 기타 포맷의 describe 정보 검증

### 6. 구현 순서 (권장)

| 순서 | 작업 | 의존성 |
|------|------|--------|
| 1 | `reader.py` + `test_reader.py` | 없음 (독립) |
| 2 | `dataset.py` + `DatasetInfo` + `test_dataset.py` | reader.py |
| 3 | `__init__.py` 변경 (`open()`, 반환 타입) | dataset.py |
| 4 | 기존 통합 테스트 업데이트 | __init__.py 변경 |
| 5 | `test_dataset_e2e.py` 신규 통합 테스트 | 전체 |
| 6 | `scripts/` 업데이트 (Dataset 사용) | 전체 |

### 7. 변경하지 않는 모듈

다음 모듈은 Phase 2에서 **변경하지 않는다** (Phase 1에서 완료, 안정 상태):

- `config.py` -- 모델/YAML I/O (Dataset이 이를 감싸서 사용)
- `detect.py` -- 포맷 감지 (그대로 사용)
- `layout_registry.py` -- 레이아웃 시스템 (그대로 사용)
- `parsers/` -- 파서 전체 (그대로 사용)
- `transforms/` -- 변환 파이프라인 전체 (그대로 사용)
- `meta.py` -- 메타 테이블 빌더 (그대로 사용)
- `export.py` -- Exporter (그대로 사용)
- `exceptions.py` -- 예외 계층 (그대로 사용)

핵심: **기존 내부 모듈은 건드리지 않고, 그 위에 Dataset 레이어를 얹는 구조.**

---

## 교훈

### 레이아웃 감지 가정 (from `fix/parser-architecture`)
초기 아키텍처는 DataGuide 6을 "와이드 vs 롱" 포맷으로 잘못 모델링했다. **실제** 분류체계: 시계열 / 스냅샷 / 커스텀. ETF 구성종목은 "시계열의 롱 버전"이 아니라 완전히 다른 데이터 카테고리였다. 새 포맷 추가 시 **유사한 가정에 주의**.

### pandas 2.x 호환성
- pandas 2.x + PyArrow 백엔드는 `StringDtype` 사용. dtype 검사에 `pd.api.types.is_string_dtype()` 사용.
- Copy-on-Write가 기본값. 체인 할당(`df["col"].iloc[0] = val`) 대신 `.loc[row, col] = val` 사용.

### 메타 테이블 설계 (from `feat/meta-export`)
- `_meta` 테이블은 **서술적(descriptive)**(실행 기록), `fnconfig.yaml`은 **처방적(prescriptive)**(사용자 의도). 역할이 다르므로 혼동하지 말 것.
- `entity_stats`는 현재 모든 테이블이 동일한 값을 공유하지만, 향후 테이블별 관리를 위해 per-table 구조 유지.

### 쓰기 전용 라이브러리의 한계 (from Phase 1 → Phase 2 전환)
Phase 1 완료 후 실제 데이터를 검증하려면 별도 스크립트(`scripts/inspect_outputs.py`)가 필요했다. 이는 라이브러리에 **읽기 인터페이스가 없다**는 구조적 문제를 드러냈다. 퀀트 워크플로우에서 "데이터 생성 → 데이터 사용"은 불가분의 사이클이므로, Dataset 핸들 패턴으로 양쪽을 통합하는 것이 자연스럽다.

---

## Git 전략 참고

- **하이브리드 Gitflow**: `feat/<name>` 브랜치를 `develop`에서 분기, `--no-ff`로 머지
- **작업 단위별 커밋**, 머지 전 테스트 실행
- **`main`** 브랜치는 v1.0까지 보류
- **항상** `uv run python -m ...`으로 실행, 단독 `python` 금지
- `pyproject.toml`에 직접 deps 편집 **금지**; `uv add` 사용
