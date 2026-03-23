# 사용자 행동 로그 기반 데이터 정합성 검증 플랫폼 구축

본 프로젝트는 Kaggle E-commerce Clickstream 데이터와 자체 Product Master 데이터를 활용하여 데이터 기반 의사결정을 위한 높은 사용재 행동 로그 (Silver Layer) 를 구축하고 이를 기반으로 전환 퍼널 분석을 수행하는 Gold Layer 마트Mart를 설계하는 것을 목표로 한다.

데이터 품질 검증을 자동화하가 위해 dbt와 Airflow를 사용하여 정합성 문제를 조기에 감지할 수 있도록 dbt tests + Failure Layer 구조를 도입한다.

## 프로젝트 목적 및 범위

### 목적

- 퍼널 분석에 활용 가능한 고품질 사용지 행동 로그 (Silver layer) 구축
- KPI 정의에 기반한 전환 분석용 Gold 마트 ('product_funnel') 개발
- dbt tests + Failure Layer 기반의 데이터 품질 검증 프레임워크 구축
- Airflow 기반의 자동화된 파이프라인 구성

### 범위

- Medallion Architecture (Raw, Bronze, Silver, Gold) 설계 및 구현
- 'slv_clickstream' 모델 구축 (핵심 사용자 행동 로그)
- 'product_funnel' Gold 마트 구축
- dbt tests 기반 품질 검증 및 'dq_failures' 저장 구조 설계
- Airflow DAG 구축
- KPI Dictionary 및 데이터 품질 규칙 정의

## Medalion Architecture

### Raw Layer ('my_dbt_project_raw')

- 원본 그대로 적재
- 'clickstream' / 'product_master' 두 테이블 구성

#### [clickstream](https://www.kaggle.com/datasets/waqi786/e-commerce-clickstream-and-transaction-dataset?utm_source=chatgpt.com)

| column name | details                                                                     |
| ----------- | --------------------------------------------------------------------------- |
| UserID      | Unique identifier for each user                                             |
| SessionID   | Unique identifier for each session.                                         |
| Timestamp   | Date and time of the interaction.                                           |
| EventType   | Type of event (e.g., page view, click, product view, add to cart, purchase) |
| ProductID   | Unique identifier for products involved in interactions.                    |
| Amount      | Amount of the transaction (for purchases).                                  |
| Outcome     | Target event (e.g., purchase).                                              |

#### product_master

| column name      | details                            |
| ---------------- | ---------------------------------- |
| proudct_id       | Unique identifier for each product |
| manufacturer     | Name of the manufacturer           |
| management_group | A category for managing products   |

### Bronze Layer ('my_dbt_project_bronze')

- 가벼운 정제 작업 (컬럼명 표준화, 타입 캐스팅)
- 비즈니스 로직 미적용 (단순 스테이징)

### Silver Layer (my_dbt_project_silver.slv_clickstream)

- 퍼널 분석을 위한 정합성 검증이 적용된 표준 이벤트 테이블
- 이벤트 순서 정렬, product validation, 타입 정제 포함
- Gold 마트 (product_funnel)의 단일 소스

| column name            | details                                                                |
| ---------------------- | ---------------------------------------------------------------------- |
| user_id                | Unique identifier for each user                                        |
| session_id             | 세션 식별자                                                            |
| event_timestamp        | 행동 발생 시각                                                         |
| event_type             | 이벤트 종류                                                            |
| product_id             | 이벤트에 연관된 상품 식별자                                            |
| amount                 | purchase 시 결제 금액                                                  |
| is_valid_event         | 품질 검증 통과 여부                                                    |
| event_order_in_sesison | 세션 내 순서                                                           |
| funnel_step            | 'page view' -> 'product_view' -> 'add_to_cart' -> 'purchase' 단계 매핑 |

--silver 레이어에 dbt 테스트를 추가하기. 한 일자 퍼널스텝의 1,2,3,4의 순서가 어긋나지 않았는지, 아니면 1, 4만 있고 중간은 없다던지 하는 유저들을 선별하기, 이런 유저들을 퍼널분석의 대상으로 삼겠다/삼지 않겠다는 로직을 validation으로 추가하기.
macros 폴더의 tests 하위디렉토리에 column_length.sql 의 내용을 test yml파일에 붙여넣고. 이 테스트를 넘긴 애들만 골드 fact table 로 넘기도록 하기.

### DQ Failure Layer (my_dbt_project_dq_failures)

- dbt test를 실패한 레코드를 자동으로 저장하는 레이어

### Gold Layer (my_dbt_project_gold.product_funnel)

- funnel 단계별 전환율 및 핵심 KPI wprhd
- 의사결정 및 리포팅을 위한 최종 MART

| column name             | details                                              |
| ----------------------- | ---------------------------------------------------- |
| event_date              | 분석 기준 일자 (이벤트 발생 날자, user_id 기준 집계) |
| page_view_users         | 해당 일자에 'page_view'를 발생시킨 고유 user 수      |
| product_view_users      | 해당 일자에 'product_view'를 발생시킨 고유 user 수   |
| add_to_cart_users       | 해당 일자에 'add_to_cart'를 발생시킨 고유 user 수    |
| purchase_users          | 해당 일자에 'purchase'를 발생시킨 고유 user 수       |
| pv_to_product_view_cvr  | \frac {'product_view_users}{'page_view_users}        |
| product_view_to_atc_cvr | \frac {'add_to_cart_users}{product_view_users}       |
| atc_to_purchase_cvr     | \frac {purchase_users}{add_to_cart_users}            |
| overall_funnel_cvr      | 전체 전환율 \frac {purchse_users}{page_view_users}   |

--fix this table and query:
--1. 일자 별 = weekly 단위로 설정.
--2. retention = 한 유저가 30일 안에 재구매한 경우. 재구매 또한 전환율의 일종으로 보고 전환율에 포함시킬것
--3. activation = 한 유저가 7일 안에 페이지를 방문하고 결제까지 완료한 경우, activation 조건 을 만족한것으로 설정한 컬럼 추가하기
--3. 위 fact table (gold layer)를 기반으로 다시 group by 를 한 새로운 테이블을 마트 MART로 간주하여 새로 창출할것 실제 분석에 그 새 마트를 사용.
-- 그로스 마케팅 시 사용하는 AARR 퍼널의 framework를 사용하여. 현 데이터상 불가능한 레퍼럴을 제외하고 retention 재방문 재구매 , activation 획득 등도 추가. (파이썬으로 그로스마케팅 분석 시 빅쿼리를 활용한 aarrr 기반 코호트 분석 등을 한다는 점에 착안하여 실제 분석에 사용할 수 있는 gold mart를 생성할것. 위 fact table은 사용해도 되고 안해도 됨. )

## 데이터 품질 목표

| 지표         | 목표    | 기준                          |
| ------------ | ------- | ----------------------------- |
| Completeness | 98%     | 필수 컬럼 채움 비율           |
| Validity     | 95%     | accepted_values, FK 일치율    |
| Timeliness   | 99%     | ETL 성공률 및 freshness       |
| MTTA         | 1 hour  | dbt test 실패 감지까지의 시간 |
| MTTR         | 24 hour | 문제 해결까지 걸린 시간       |

## 기능 요구사항

- silver layer 구축 (my_dbt_project_silver.slv_clickstream)
  - Raw/Bronze 클릭스트림을 기반으로 표준화된 사용자 행동 로그 테이플 생성
  - 이벤트 Timestamp 기준 정렬 및 세션 단위 이벤트 시퀀스 (event_order_in_session) 생성
  - event_type 을 퍼널 단계(funnel_step) 으로 매핑
    - 1: page_view -> 2: product_view -> 3: add_to_cart -> 4: purchase
  - product_master 를 활용한 product_id 기준 FK 조인 구조 마련
  - 이벤트 품질 상태를 나타내는 is_valid_event 플래그 계산
    - 필수 컬럼 존재 여부, 이벤트 타입 유효 여부, FK 유효 여부, purchase 금액 검증 등 반영
- 데이터 품질 검증 (dbt tests on slv_clickstream)
  - not_null (user_id, session_id, event_type, event_timestamp)
  - accepted_values (event_type)
  - product_id FK validation (relationships test)
  - timestamp ordering test (Custom)
    - 동일 user_id, session_id 내에서 event_timestamp가 역전되지 않음을 검증
  - purchase 이벤트 시 amount > 0 검증 (Custom)
  - (확장가능) event 타입 별 논리 검증
    - 예: add_to_cart 이전에 product_view 존재 여부, purchase 이전 단계 선행 이벤트 존재 여부 등
  - 테스트 실패 시
    - 실패한 row는 'my_dbt_project_dq_failures' 스키마에 저장
- Gold 마트 구축 (my_dbt_project_gold.proudct_funnel)
  - Silver 이벤트 로그에서 퍼널 단계별 사용자수 집계
  - KPI (전환율, 단계별 이탈 등)를 계산하여 최종 마트로 반영
  - 전환율 정합성 검증 ( 0 <= 'conversion_rate' <= 1)

## 비기능 요구사항

- Gold 마트는 매일 7am 이전에 준비 완료
- Airflow 성공률 99% 이상
- 테스트 실패 건수 추적 및 품질 개선 지표 운영
- 새로운 이벤트 타입 추가 시 Silver/Gold 스키마 확장 가능해야 함

## 시스템 구성도
