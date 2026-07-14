# 붕어빵 공장 시뮬레이터

붕어빵 제조라인의 적외선 온도 / 실내온습도를 흉내내는 가상 공장
시뮬레이터입니다. 하나의 자바(JVM) 프로세스 안에서 **라인 여러 개(기본 7개)** 를
동시에 띄우고, 라인마다 다음 두 가지 산업용 프로토콜을 서비스합니다.

- **Modbus TCP 서버** (j2mod) — 적외선 온도계 실측온도 같은 "직결형(하드와이어드)
  센서"를 레지스터로 노출. **라인마다 독립된 TCP 포트**를 연다 (최신 PLC가 라인마다 자체
  IP/포트를 갖는 구성을 흉내낸 것 — 라인 1은 502, 라인 2는 503, ...).
- **MQTT** (Eclipse Paho 클라이언트 + Moquette **임베디드 브로커**) — 실내온습도 같은
  "최신 IoT형 센서" 값을 JSON으로 발행. 실내온습도 센서는 라인마다 있는 게 아니라
  건물 전체에 하나뿐이라고 가정했기 때문에, 라인 수와 무관하게 브로커 하나에 **토픽
  하나**(`factory/roomEnv`)로만 발행한다.

Moquette를 프로세스 안에 내장했기 때문에, mosquitto 같은 외부 MQTT 브로커를 따로
설치하지 않아도 `./gradlew :sim:run` 한 번으로 브로커 + 발행자(publisher, 프로세스당 1개)가
전부 뜹니다.

## 전체 구조

하나의 Git 리포지토리(모노레포) 안에서, 실제로 배포/실행되는 단위별로 Gradle 모듈을
나눠뒀습니다. 모듈이 다르면 서로 다른 프로세스(사실상 서로 다른 서버)로 독립 실행되고,
필요한 의존성만 각자 가집니다.

```
generator/
 ├─ common/            여러 모듈이 공유하는 순수 유틸 (JsonFields - JSON 필드 파싱)
 ├─ sim/                공장 시뮬레이터 본체 (Modbus TCP 서버 + 임베디드 MQTT 브로커)
 │   └─ com.factory.sim
 │       ├─ FactoryState.java        공유 상태 (라인 1개분 물리값의 유일한 보관소)
 │       ├─ RoomEnvironment.java     공유 상태 (실내온습도, 공장 전체에 딱 1개뿐인 값의 보관소)
 │       ├─ PhysicsSimulator.java    0.5초마다 물리 상태를 계산/갱신하는 전용 스레드 (라인당 1개)
 │       ├─ RoomEnvSimulator.java    0.5초마다 실내온습도를 랜덤워크로 갱신하는 전용 스레드 (프로세스당 1개)
 │       ├─ Main.java                진입점 - 라인 N개(기본 7)를 루프로 띄우고 MQTT 브로커/온습도는 공유
 │       ├─ modbus/                  ModbusServer 등 j2mod 기반 Modbus TCP 서버
 │       └─ mqtt/                    EmbeddedBroker(Moquette), RoomEnvPublisher(프로세스당 1개)
 ├─ tools/              개발용 CLI 테스트 클라이언트 (ModbusTestClient, MqttTestClient, ModbusJsonPoller)
 ├─ kafka-bridge/        Modbus/MQTT 값을 읽어 Kafka로 직접 발행하는 브릿지
 ├─ linestate-streams/   factory.modbus + factory.roomenv를 join/정규화하는 Kafka Streams 앱
 ├─ db-sink/             factory.linestate를 TimescaleDB에 적재하는 콜드 패스 백엔드
 ├─ ws-server/           factory.linestate를 브라우저로 relay하는 WebSocket 핫 패스 백엔드
 ├─ history-server/      TimescaleDB 이력을 HTTP로 조회하는 서버
 └─ web/                 정적 프론트엔드 (index.html, live-monitor.html) - 파일로 직접 열어서 사용
```

라인 하나(`FactoryState` + `PhysicsSimulator` + `ModbusServer`)가 "PLC 한 대짜리 생산
라인"에 대응한다. `Main`은 이 묶음을 라인 개수만큼 만들어서 각각 독립적으로 기동한다 —
값 흐름 규칙(아래 참고)은 라인마다 완전히 분리되어 있다.

반면 실내온습도(`RoomEnvironment` + `RoomEnvSimulator` + `RoomEnvPublisher`)는 라인에 속한
값이 아니라 건물 전체가 공유하는 값이라서, 라인 개수와 무관하게 `Main`이 프로세스 전체에서
딱 한 번만 만든다. 결국 라인 간에 공유되는 건 MQTT 브로커 프로세스 하나와 이 실내온습도
값 하나, 둘뿐이다.

### 값이 흐르는 방향 (읽기 전용 규칙)

```
[제어값]     Modbus 마스터(외부) --쓰기--> FactoryState     --읽기--> (없음, 벨트속도는 SCADA 전용 제어값)
[센서값]     PhysicsSimulator   --쓰기--> FactoryState     --읽기--> Modbus 서버
[실내온습도] RoomEnvSimulator   --쓰기--> RoomEnvironment  --읽기--> MQTT
```

`FactoryState`는 센서값(적외선 실측온도)을 쓰는 setter를
**패키지 전용(package-private)**으로 감춰뒀습니다. 그래서 같은 패키지에 있는
`PhysicsSimulator`만 그 값을 바꿀 수 있고, 하위 패키지에 있는 `modbus`/`mqtt` 쪽
클래스는 컴파일 타임부터 아예 호출이 불가능합니다 — "물리 상태는 한 곳에서만
계산하고, 나머지는 읽기만 한다"는 규칙이 코드 구조로 강제됩니다. `RoomEnvironment`도
같은 규칙으로, `RoomEnvSimulator`만 쓸 수 있습니다.

반대로 벨트속도 같은 "제어값"은 Modbus 마스터가 쓰기만 하는 반대 방향의 흐름입니다 -
적외선 온도계는 더 이상 이 제어값의 영향을 받지 않는 순수 측정값입니다.

모든 값은 `AtomicInteger`/`AtomicReference`로 감싸서, 물리 스레드/Modbus 접속
스레드들/MQTT 발행 스레드가 동시에 접근해도 값이 깨지지 않도록(스레드 안전) 만들었습니다.

## Modbus 레지스터 맵

레지스터 주소는 **모든 라인이 동일**하고, 라인은 **포트로 구분**한다 (기본: 라인 n → 포트
`502 + n - 1`, 즉 라인 1=502, 라인 2=503, ..., 라인 7=508). Unit ID는 라인 내부에서 항상 1.

| 종류 | FC | addr | 내용 | 스케일 |
|---|---|---|---|---|
| Input Register (읽기전용) | 0x04 | 0 | 적외선 온도계 실측온도 | x10 (2000 = 200.0°C) |
| Holding Register (읽기/쓰기) | 0x03 / 0x06 | 0 | 벨트속도 지령 | x100 (140 = 1.40Hz) |

## MQTT 토픽

- 토픽: `factory/roomEnv` (라인 수와 무관하게 딱 하나 - 실내온습도 센서가 건물 전체에
  하나뿐이라는 가정)
- payload 예시: `{"temp":22.3,"humidity":51.2,"ts":1719900000000}`
- 발행 주기: 2초
- 브로커: 이 프로세스 안에서 뜨는 임베디드 Moquette 1개를 **모든 라인이 공유** (기본
  `tcp://localhost:1883`)

## 물리 모델 요약

- `irTemp`(적외선 온도계 실측온도)는 라인마다 독립적으로 아주 느린 랜덤워크로 흔들리는
  순수 측정값 (더 이상 목표온도(SV) 같은 외부 제어를 따라가지 않음)
- `roomTemp`, `roomHumidity`는 (라인마다가 아니라 공장 전체에서 딱 한 번) 매우 느린
  랜덤워크로 미세하게 흔들림
- 실측값에는 센서 오차를 흉내낸 ±0.3 정도의 랜덤 노이즈를 더함
- 모든 실측값은 16비트 부호있는 정수 범위(-32768~32767)로 클램핑되어 레지스터에 노출됨

## 실행 방법

JDK 17 이상이 필요합니다. (Gradle 9 wrapper가 이미 포함되어 있어서 별도로 Gradle을
설치할 필요는 없습니다.)

### 1) 가장 간단한 방법 — `./gradlew :sim:run`

```bash
./gradlew :sim:run
```

기본적으로 라인은 **7개**, Modbus는 라인 1이 `502` 포트(라인마다 +1씩), MQTT는 `1883`
포트를 사용합니다. 라인 개수와 포트는 시스템 프로퍼티로 바꿀 수 있습니다.

```bash
./gradlew :sim:run -Dlines=7 -Dmodbus.basePort=1502 -Dmqtt.port=11883
```

위 예시라면 라인 1은 Modbus `1502`, 라인 2는 `1503`, ..., 라인 7은 `1508`을 씁니다.

> **참고 (Windows + 한글 로캘 콘솔에서 한글 로그가 깨져 보이는 경우):**
> 이건 프로그램의 버그가 아니라, Gradle이 자식 프로세스의 콘솔 출력을 가로채서
> 자기 방식대로 다시 인코딩하는 과정에서 생기는 표시상의 문제입니다 (실제 동작·값은
> 전혀 영향받지 않습니다). 깨끗한 한글 로그를 보고 싶다면 아래 2번 방법을 쓰세요.

### 2) 콘솔 한글이 안 깨지는 방법 — `installDist`로 실행 스크립트를 만들어 직접 실행

```bash
./gradlew :sim:installDist
./sim/build/install/sim/bin/sim.bat      # Windows
./sim/build/install/sim/bin/sim          # Linux/macOS
```

이 경우 Gradle이 아니라 생성된 실행 스크립트가 `java.exe`를 직접 호출하기 때문에
콘솔 출력이 Gradle을 거치지 않고 그대로 나와서 한글이 깨지지 않습니다.

### 종료

터미널에서 `Ctrl+C`를 누르면 셧다운 훅이 각 라인을 역순으로(Modbus 서버 → 물리 시뮬레이션)
정리하고, 이어서 공유 실내온습도 발행자/시뮬레이터를 내린 뒤, 마지막으로 공유 MQTT
브로커를 내리고 종료합니다.

## 테스트 클라이언트 실행

시뮬레이터(`./gradlew :sim:run` 등)가 먼저 떠 있는 상태에서, **다른 터미널**을 열어
아래 명령을 실행하세요.

```bash
# Modbus: 값 읽기 + 벨트속도 지령 쓰기 테스트
./gradlew :tools:runModbusTestClient

# MQTT: 임베디드 브로커에 구독해서 실내온습도 메시지 도착 확인
./gradlew :tools:runMqttTestClient
```

- `ModbusTestClient`는 Input Register(적외선 실측온도)를 읽고, Holding
  Register의 벨트속도 지령을 2.00Hz로 바꿔 쓴 뒤, 다시 읽어서 반영됐는지 확인합니다.
  기본은 라인 1(포트 502)에 접속하며, 다른 라인을 보려면 두 번째 인자로 포트를 바꿉니다:
  `./gradlew :tools:runModbusTestClient --args="localhost 503"` (라인 2).
- `MqttTestClient`는 `factory/roomEnv` 토픽(공장 전체가 공유하는 실내온습도, 딱 하나)을
  구독해서 메시지가 3개 도착할 때까지(최대 30초) 기다리고, 수신된 JSON 내용을 출력합니다.

포트를 바꿨다면 동일하게 `-Dmodbus.basePort=`, `-Dmqtt.port=` 옵션을 붙이면 됩니다.

## Kafka + Kafka UI 파이프라인 (선택)

시뮬레이터(Modbus + MQTT)는 그 자체로 "필드 장비"에 해당한다. 여기에 NiFi나 Node-RED 같은
별도 ETL 툴 없이, `KafkaBridge`(자바 프로세스, `kafka-bridge` 모듈)가 직접 Modbus를
폴링하고 MQTT를 구독해서 Kafka로 발행하는 구성을 `docker-compose.yml` + `:kafka-bridge:run`
태스크로 준비해뒀다. Kafka에
실제로 데이터가 흐르는지는 **Kafka UI**(브라우저 웹 UI)로 눈으로 확인한다.

### 왜 이런 구조인가

- 라인 7개는 **최신 PLC 7대**를 흉내낸 것이라, 각자 자기 IP:포트를 가진 독립된 장비다.
  그래서 `KafkaBridge`도 라인마다 **별도의 Modbus TCP 커넥션**을 연다 — 번거롭지만 실제
  현장도 그렇다.
- Kafka 토픽은 라인별로 나누지 않고 **`factory.modbus`**, **`factory.roomenv`** 두 개로
  통합한다. `factory.modbus`는 라인마다 값이 달라서 메시지 **키(key)를 lineId**로 써서
  같은 라인의 데이터가 같은 파티션에 순서대로 쌓이게 한다. 라인이 늘어나도(7→20) 토픽
  개수가 안 늘어나는 게 실무 Kafka에서 흔한 패턴이다. 반면 `factory.roomenv`는 애초에
  라인별 값이 아니라 공장 전체 값 하나뿐이라서 고정된 키(`factory`) 하나만 쓴다.
- `KafkaBridge`는 Modbus(폴링 방식)와 MQTT(구독 방식) **둘 다** 수집해서 같은 Kafka
  클러스터로 흘려보낸다 — 서로 다른 프로토콜을 하나의 스트리밍 백본(Kafka)으로 모으는
  전형적인 IIoT 게이트웨이 구성을, 외부 툴 대신 기존 프로젝트의 `ModbusJsonPoller`/
  `RoomEnvPublisher`와 같은 스타일의 작은 자바 클래스로 직접 구현한 것이다.

### 기동

```bash
docker compose up -d
docker compose ps
```

- Kafka: 같은 docker network 안(kafka-ui 등)에서는 `kafka:9092`, 호스트에서는
  `localhost:9094`로 접속.
- Kafka UI: 브라우저에서 `http://localhost:8080`.

### 브릿지 + 시뮬레이터 실행

터미널 두 개가 필요하다 (둘 다 컨테이너 밖 호스트에서 실행).

```bash
./gradlew :sim:run           # 터미널 1: 7라인 시뮬레이터 (Modbus TCP 502~508 + 임베디드 MQTT 1883)
./gradlew :kafka-bridge:run  # 터미널 2: Modbus/MQTT 값을 읽어 Kafka로 발행하는 브릿지
```

`KafkaBridge`는 라인마다 Modbus TCP에 접속해서 1초 간격으로 Input Register(적외선
실측온도)를 읽어 `factory.modbus`에 키=`line1`~`line7`로 발행하고, 임베디드
브로커의 `factory/roomEnv` 토픽(공장 전체 공유 실내온습도, 하나뿐) 하나를 구독해서 받은
메시지를 그대로 `factory.roomenv`에 고정 키(`factory`)로 발행한다.

### 확인

- 브라우저로 `http://localhost:8080` (Kafka UI) 접속 → Topics 메뉴에서 `factory.modbus`,
  `factory.roomenv` 두 토픽을 연다. `factory.modbus`는 key=line1~line7 메시지가,
  `factory.roomenv`는 key=factory 메시지가 계속 새로 들어오는지, 값(적외선온도,
  온습도)이 시간에 따라 자연스럽게 바뀌는지 확인한다.
- 콘솔로 교차 확인하려면:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic factory.modbus --from-beginning

docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic factory.roomenv --from-beginning
```

### 정규화(Kafka Streams) + 실시간 WebSocket 릴레이 + TimescaleDB 이력

`factory.modbus`(라인마다 1초마다 확정 발행되는 스트림)와 `factory.roomenv`(공장 전체가
공유하는 최신값 하나짜리 GlobalKTable)를 join해서, 하나의 통일된 스키마로
`factory.linestate`에 다시 쓰는 Kafka Streams 앱(`linestate-streams` 모듈)이 있다. 그 결과를 각각 독립된
컨슈머 그룹으로 구독하는 두 갈래가 있다: 저장 없이 곧바로 브라우저로 relay하는 핫 패스
(`ws-server`)와, TimescaleDB에 적재하는 콜드 패스(`db-sink`). 저장된 이력은
`history-server`가 HTTP로 조회할 수 있게 연다.

```bash
./gradlew :linestate-streams:run   # 터미널 3: factory.modbus + factory.roomenv -> factory.linestate
./gradlew :ws-server:run           # 터미널 4: factory.linestate -> WebSocket(8081)으로 브라우저에 relay
./gradlew :db-sink:run             # 터미널 5: factory.linestate -> TimescaleDB(line_state)에 적재
./gradlew :history-server:run      # 터미널 6: GET /history?lineId=line1&minutes=15 (포트 8083)
```

- `factory.linestate` 메시지 예: `{"lineId":"line1","irTemp":200.3,"roomTemp":21.1,"roomHumidity":40.5,"ts":...}`
- 브라우저에서 `web/live-monitor.html`을 열면(파일 직접 열기, `ws://localhost:8081`에 접속)
  라인별 카드에 적외선온도, 실내온습도가 실시간으로 계속 갱신되는 걸 확인할
  수 있다. 카드를 클릭하면 `history-server`(포트 8083)에서 최근 이력을 가져와 보여준다.

### 각 단계의 장애를 구분하기 위한 예외 처리 + Prometheus 메트릭

Kafka consumer group lag만 보면 "이 단계가 밀리고 있다"는 건 알 수 있어도, "왜" 밀리는지는
구분이 안 된다(예: 센서 자체가 응답이 없는 것과, 센서는 정상인데 Kafka 전송만 실패하는 것은
관리자 입장에서 조치 방법이 완전히 다르다). 그래서 `kafka-bridge` / `linestate-streams` /
`db-sink` 세 프로세스는 각자 실패 지점을 세분화해서 자체 `/metrics` 엔드포인트로 노출한다
(`prometheus/prometheus.yml`에 `host.docker.internal:포트`로 이미 스크레이프 등록됨).

| 프로세스 | 포트 | 노출 지표 | 실패 시 동작 |
|---|---|---|---|
| `kafka-bridge` | 9101 | `bridge_modbus_poll_total{line,result}`, `bridge_modbus_connected{line}`, `bridge_kafka_send_total{topic,result}`, `bridge_mqtt_connected`, `bridge_mqtt_reconnect_total` | Modbus 접속 끊김은 라인별로 다음 tick에 재접속 시도. MQTT는 Paho의 자동 재접속(`connectionLost`/`connectComplete` 콜백으로 상태 추적). Kafka 전송 실패는 `producer.send()` 콜백으로 잡아서 카운트(과거엔 콜백이 없어서 조용히 유실됐음). |
| `linestate-streams` | 9102 | `streams_state`(2=정상, 1=시작/리밸런싱, 0=다운), `streams_uncaught_exceptions_total` | `StreamsUncaughtExceptionHandler`를 등록해 브로커 일시 단절 같은 예외로 스트림 스레드가 죽어도 프로세스 전체가 아니라 스레드만 교체(`REPLACE_THREAD`)해서 계속 재시도. |
| `db-sink` | 9103 | `dbsink_insert_batches_total{result}`, `dbsink_records_inserted_total`, `dbsink_db_connected`, `dbsink_reconnect_total` | 배치 insert/commit 중 `SQLException`이 나면(과거엔 여기서 프로세스가 죽었음) 연결을 닫고 지수 백오프(1초→최대 30초)로 재연결하며 폴링 루프를 계속 이어감. 실패한 배치의 레코드는 재처리하지 않고 유실됨. |

Grafana 쪽에서 이 지표들을 아직 전용 패널로는 안 붙였다 — 지금은 코드 레벨에서 "죽지 않고
버티면서 원인별로 카운트한다"까지만 되어 있고, `pipeline-flow`/`pipeline-health` 대시보드는
여전히 Kafka consumer group lag 기반이다.

## 사용한 라이브러리

| 용도 | 라이브러리 | 버전 |
|---|---|---|
| Modbus TCP 서버/클라이언트 | `com.ghgande:j2mod` | 3.3.0 |
| MQTT 클라이언트 | `org.eclipse.paho:org.eclipse.paho.client.mqttv3` | 1.2.5 |
| 임베디드 MQTT 브로커 | `io.moquette:moquette-broker` | 0.17 |
| Kafka producer (KafkaBridge용) | `org.apache.kafka:kafka-clients` | 3.8.0 |
| Kafka Streams (정규화 조인용) | `org.apache.kafka:kafka-streams` | 3.8.0 |
| WebSocket 서버 (실시간 relay용) | `org.java-websocket:Java-WebSocket` | 1.5.6 |
| Prometheus 클라이언트 (kafka-bridge/linestate-streams/db-sink `/metrics`) | `io.prometheus:simpleclient`, `simpleclient_httpserver` | 0.16.0 |
