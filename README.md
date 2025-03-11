### 추가 기간 처리에 대응

새로운 기간의 데이터를 추가할 때도 기존 데이터와의 연속성(Session ID)을 유지할 수 있도록 하였습니다.

### 배치 장애시 복구를 위한 장치

배치 장애 발생 시 backfill 작업을 고려하여 멱등성을 보장하고, partition overwrite를 수행하였습니다.
또한, 한국 시간(KST) 기준으로 데이터를 overwrite하기 때문에, 배치 작업 시 KST와 UTC의 9시간 차이를 고려하여 해당 시간 범위의 데이터도 포함하도록 하였습니다.

### 10월, 11월 User ID 기준 WAU

```
WITH user_activity_with_week AS (
    SELECT
        user_id,
        DATE_TRUNC('WEEK', event_date_kst) AS event_week
    FROM user_activity
)

SELECT
    event_week,
    COUNT(DISTINCT user_id) AS wau
FROM user_activity_with_week
WHERE
    event_week >= DATE_TRUNC('WEEK', '2019-09-30')
        AND event_week <= DATE_TRUNC('WEEK', '2019-11-25')
GROUP BY event_week
ORDER BY event_week ASC;
```

| event_week          | wau     |
|---------------------|---------|
| 2019-09-30 00:00:00 | 818388  |
| 2019-10-07 00:00:00 | 1057958 |
| 2019-10-14 00:00:00 | 1090898 |
| 2019-10-21 00:00:00 | 1093146 |
| 2019-10-28 00:00:00 | 1054722 |
| 2019-11-04 00:00:00 | 1321141 |
| 2019-11-11 00:00:00 | 1543309 |
| 2019-11-18 00:00:00 | 1376755 |
| 2019-11-25 00:00:00 | 1176254 |

### 10월, 11월 Session ID 기준 WAU

```
WITH user_activity_with_week AS (
    SELECT
        session_id,
        DATE_TRUNC('WEEK', event_date_kst) AS event_week
    FROM user_activity
)

SELECT
    event_week,
    COUNT(DISTINCT session_id) AS wau
FROM user_activity_with_week
WHERE
    event_week >= DATE_TRUNC('WEEK', '2019-09-30')
    AND event_week <= DATE_TRUNC('WEEK', '2019-11-25')
GROUP BY event_week
ORDER BY event_week ASC;
```

| event_week          | wau     |
|---------------------|---------|
| 2019-09-30 00:00:00 | 1570536 |
| 2019-10-07 00:00:00 | 2154180 |
| 2019-10-14 00:00:00 | 2257214 |
| 2019-10-21 00:00:00 | 2153837 |
| 2019-10-28 00:00:00 | 2115233 |
| 2019-11-04 00:00:00 | 2751842 |
| 2019-11-11 00:00:00 | 4754423 |
| 2019-11-18 00:00:00 | 2876494 |
| 2019-11-25 00:00:00 | 2376156 |

### Scala를 사용한 이유

Spark가 Scala로 작성된 만큼, Spark API는 Scala 문법에서 가장 효율적이고 가독성 있게 작성될 수 있습니다.
조금 더 자세히 설명하자면, 다음과 같은 이유로 Spark Application을 개발할 때 Scala 언어로 작성하는 것이 생산적입니다.

- DataFrame의 체이닝 (예: `.withColumn`)을 통해 코드 가독성을 높일 수 있습니다.
- 다양한 연산자 오버로딩 (예: $"event_time")을 통해 코드를 간결하게 유지할 수 있습니다.
- Implicit Conversion을 통해 커스텀 함수를 생성하더라도 체이닝 형태의 컨벤션을 유지할 수 있습니다.
- object를 활용하여 프레임워크 없이도 싱글톤 사용이 가능합니다.
- 함수형 프로그래밍 언어로서 불필요한 코드 작성을 피할 수 있습니다.
- SQL 작성 시 String Interpolation 덕분에 가독성이 높아집니다.
