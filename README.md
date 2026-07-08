# 붕어빵 공장 시뮬레이터

붕어빵 제조라인의 화력온도 / 몰드(틀)온도 / 실내온습도 / 생산개수를 흉내내는 가상 공장
시뮬레이터입니다. 하나의 자바(JVM) 프로세스 안에서 **라인 여러 개(기본 7개)** 를
동시에 띄우고, 라인마다 다음 두 가지 산업용 프로토콜을 서비스합니다.

- **Modbus TCP 서버** (j2mod) — 화력/몰드온도, 생산개수 같은 "직결형(하드와이어드) 센서"를
  레지스터로 노출. **라인마다 독립된 TCP 포트**를 연다 (최신 PLC가 라인마다 자체
  IP/포트를 갖는 구성을 흉내낸 것 — 라인 1은 502, 라인 2는 503, ...).
- **MQTT** (Eclipse Paho 클라이언트 + Moquette **임베디드 브로커**) — 실내온습도 같은
  "최신 IoT형 센서" 값을 JSON으로 발행. 브로커는 모든 라인이 공유하는 대신, 라인마다
  **토픽**을 다르게 써서 구분한다 (`factory/line{n}/roomEnv`).

Moquette를 프로세스 안에 내장했기 때문에, mosquitto 같은 외부 MQTT 브로커를 따로
설치하지 않아도 `./gradlew run` 한 번으로 브로커 + 발행자(publisher, 라인당 1개)가
전부 뜹니다.

## 전체 구조

```
com.factory.sim
 ├─ FactoryState.java        공유 상태 (라인 1개분 물리값의 유일한 보관소)
 ├─ PhysicsSimulator.java    0.5초마다 물리 상태를 계산/갱신하는 전용 스레드 (라인당 1개)
 ├─ Main.java                진입점 - 라인 N개(기본 7)를 루프로 띄우고 MQTT 브로커는 공유
 ├─ modbus/
 │   ├─ ModbusServer.java            j2mod 기반 Modbus TCP 서버 (라인당 1개, 포트 다름)
 │   ├─ DelegatingInputRegister.java Input Register 읽기 전용 어댑터
 │   └─ DelegatingRegister.java      Holding Register 읽기/쓰기 어댑터
 ├─ mqtt/
 │   ├─ EmbeddedBroker.java     Moquette 임베디드 MQTT 브로커 (전체 라인 공유, 1개)
 │   └─ RoomEnvPublisher.java   실내온습도를 2초마다 발행하는 Paho 클라이언트 (라인당 1개)
 ├─ bridge/
 │   └─ KafkaBridge.java    Modbus/MQTT 값을 읽어 Kafka로 직접 발행하는 브릿지 (별도 프로세스)
 └─ client/                 (테스트/보조용 독립 실행 클라이언트)
     ├─ ModbusTestClient.java
     ├─ MqttTestClient.java
     └─ ModbusJsonPoller.java
```

라인 하나(`FactoryState` + `PhysicsSimulator` + `ModbusServer` + `RoomEnvPublisher`)가
"PLC 한 대짜리 생산 라인"에 대응한다. `Main`은 이 묶음을 라인 개수만큼 만들어서 각각
독립적으로 기동한다 — 값 흐름 규칙(아래 참고)은 라인마다 완전히 분리되어 있고, 라인 간에
공유되는 건 MQTT 브로커 프로세스 하나뿐이다(토픽으로만 구분).

### 값이 흐르는 방향 (읽기 전용 규칙)

```
[제어값 SV]  Modbus 마스터(외부) --쓰기--> FactoryState --읽기--> PhysicsSimulator
[센서값]     PhysicsSimulator   --쓰기--> FactoryState --읽기--> Modbus 서버 / MQTT
```

`FactoryState`는 센서값(화력/몰드 실측온도, 생산개수, 실내온습도)을 쓰는 setter를
**패키지 전용(package-private)**으로 감춰뒀습니다. 그래서 같은 패키지에 있는
`PhysicsSimulator`만 그 값을 바꿀 수 있고, 하위 패키지에 있는 `modbus`/`mqtt` 쪽
클래스는 컴파일 타임부터 아예 호출이 불가능합니다 — "물리 상태는 한 곳에서만
계산하고, 나머지는 읽기만 한다"는 규칙이 코드 구조로 강제됩니다.

반대로 목표온도(SV)·벨트속도 같은 "제어값"은 Modbus 마스터가 쓰고
PhysicsSimulator가 읽기만 하는, 반대 방향의 흐름입니다.

모든 값은 `AtomicInteger`/`AtomicReference`로 감싸서, 물리 스레드/Modbus 접속
스레드들/MQTT 발행 스레드가 동시에 접근해도 값이 깨지지 않도록(스레드 안전) 만들었습니다.

## Modbus 레지스터 맵

레지스터 주소는 **모든 라인이 동일**하고, 라인은 **포트로 구분**한다 (기본: 라인 n → 포트
`502 + n - 1`, 즉 라인 1=502, 라인 2=503, ..., 라인 7=508). Unit ID는 라인 내부에서 항상 1.

| 종류 | FC | addr | 내용 | 스케일 |
|---|---|---|---|---|
| Input Register (읽기전용) | 0x04 | 0 | 화력 실측온도 | x10 (2200 = 220.0°C) |
| Input Register (읽기전용) | 0x04 | 1 | 몰드 실측온도 | x10 |
| Input Register (읽기전용) | 0x04 | 2 | 생산개수 누적 | 정수 |
| Holding Register (읽기/쓰기) | 0x03 / 0x06 | 0 | 화력 목표온도(SV) | x10 |
| Holding Register (읽기/쓰기) | 0x03 / 0x06 | 1 | 벨트속도 지령 | x100 (140 = 1.40Hz) |

## MQTT 토픽

- 토픽: `factory/line{n}/roomEnv` (예: 라인 1 = `factory/line1/roomEnv`, 라인 7 =
  `factory/line7/roomEnv`)
- payload 예시: `{"temp":22.3,"humidity":51.2,"ts":1719900000000}`
- 발행 주기: 2초
- 브로커: 이 프로세스 안에서 뜨는 임베디드 Moquette 1개를 **모든 라인이 공유** (기본
  `tcp://localhost:1883`)

## 물리 모델 요약

- `actFireTemp`가 목표온도(SV)를 향해 시정수 3초로 서서히 수렴
- `targetMold = actFireTemp * 0.90 + roomTemp * 0.10`
- `moldTemp`가 `targetMold`를 향해 시정수 8초로 서서히 수렴 (화력보다 느리게 반응)
- `roomTemp`, `roomHumidity`는 매우 느린 랜덤워크로 미세하게 흔들림
- 실측값에는 센서 오차를 흉내낸 ±0.3 정도의 랜덤 노이즈를 더함
- `moldTemp`가 180°C 이상일 때만 낮은 확률로 생산개수가 1개씩 증가 (PLC 카운터 흉내)
- 모든 실측값은 16비트 부호있는 정수 범위(-32768~32767)로 클램핑되어 레지스터에 노출됨

## 실행 방법

JDK 17 이상이 필요합니다. (Gradle 9 wrapper가 이미 포함되어 있어서 별도로 Gradle을
설치할 필요는 없습니다.)

### 1) 가장 간단한 방법 — `./gradlew run`

```bash
./gradlew run
```

기본적으로 라인은 **7개**, Modbus는 라인 1이 `502` 포트(라인마다 +1씩), MQTT는 `1883`
포트를 사용합니다. 라인 개수와 포트는 시스템 프로퍼티로 바꿀 수 있습니다.

```bash
./gradlew run -Dlines=7 -Dmodbus.basePort=1502 -Dmqtt.port=11883
```

위 예시라면 라인 1은 Modbus `1502`, 라인 2는 `1503`, ..., 라인 7은 `1508`을 씁니다.

> **참고 (Windows + 한글 로캘 콘솔에서 한글 로그가 깨져 보이는 경우):**
> 이건 프로그램의 버그가 아니라, Gradle이 자식 프로세스의 콘솔 출력을 가로채서
> 자기 방식대로 다시 인코딩하는 과정에서 생기는 표시상의 문제입니다 (실제 동작·값은
> 전혀 영향받지 않습니다). 깨끗한 한글 로그를 보고 싶다면 아래 2번 방법을 쓰세요.

### 2) 콘솔 한글이 안 깨지는 방법 — `installDist`로 실행 스크립트를 만들어 직접 실행

```bash
./gradlew installDist
./build/install/generator/bin/generator.bat      # Windows
./build/install/generator/bin/generator          # Linux/macOS
```

이 경우 Gradle이 아니라 생성된 실행 스크립트가 `java.exe`를 직접 호출하기 때문에
콘솔 출력이 Gradle을 거치지 않고 그대로 나와서 한글이 깨지지 않습니다.

### 종료

터미널에서 `Ctrl+C`를 누르면 셧다운 훅이 각 라인을 역순으로(Modbus 서버 → MQTT 발행자 →
물리 시뮬레이션) 정리한 뒤, 마지막으로 공유 MQTT 브로커를 내리고 종료합니다.

## 테스트 클라이언트 실행

시뮬레이터(`./gradlew run` 등)가 먼저 떠 있는 상태에서, **다른 터미널**을 열어
아래 명령을 실행하세요.

```bash
# Modbus: 값 읽기 + 화력목표(SV) 쓰기 테스트
./gradlew runModbusTestClient

# MQTT: 임베디드 브로커에 구독해서 실내온습도 메시지 도착 확인
./gradlew runMqttTestClient
```

- `ModbusTestClient`는 Input Register(화력/몰드 실측온도, 생산개수)를 읽고, Holding
  Register의 화력목표(SV)를 200.0°C로 바꿔 쓴 뒤, 다시 읽어서 반영됐는지 확인합니다.
  기본은 라인 1(포트 502)에 접속하며, 다른 라인을 보려면 두 번째 인자로 포트를 바꿉니다:
  `./gradlew runModbusTestClient --args="localhost 503"` (라인 2).
- `MqttTestClient`는 `factory/+/roomEnv` 와일드카드 토픽을 구독해서 **모든 라인**의
  메시지가 합쳐서 3개 도착할 때까지(최대 30초) 기다리고, 수신된 토픽/JSON 내용을
  출력합니다 (토픽명으로 어느 라인인지 구분).

포트를 바꿨다면 동일하게 `-Dmodbus.basePort=`, `-Dmqtt.port=` 옵션을 붙이면 됩니다.

## Kafka + Kafka UI 파이프라인 (선택)

시뮬레이터(Modbus + MQTT)는 그 자체로 "필드 장비"에 해당한다. 여기에 NiFi나 Node-RED 같은
별도 ETL 툴 없이, `KafkaBridge`(자바 프로세스)가 직접 Modbus를 폴링하고 MQTT를 구독해서
Kafka로 발행하는 구성을 `docker-compose.yml` + `runKafkaBridge` 태스크로 준비해뒀다. Kafka에
실제로 데이터가 흐르는지는 **Kafka UI**(브라우저 웹 UI)로 눈으로 확인한다.

### 왜 이런 구조인가

- 라인 7개는 **최신 PLC 7대**를 흉내낸 것이라, 각자 자기 IP:포트를 가진 독립된 장비다.
  그래서 `KafkaBridge`도 라인마다 **별도의 Modbus TCP 커넥션**을 연다 — 번거롭지만 실제
  현장도 그렇다.
- Kafka 토픽은 라인별로 나누지 않고 **`factory.modbus`**, **`factory.roomenv`** 두 개로
  통합한다. 대신 Kafka 메시지 **키(key)를 lineId**로 써서 같은 라인의 데이터가 같은
  파티션에 순서대로 쌓이게 한다. 라인이 늘어나도(7→20) 토픽 개수가 안 늘어나는 게 실무
  Kafka에서 흔한 패턴이다.
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
./gradlew run              # 터미널 1: 7라인 시뮬레이터 (Modbus TCP 502~508 + 임베디드 MQTT 1883)
./gradlew runKafkaBridge   # 터미널 2: Modbus/MQTT 값을 읽어 Kafka로 발행하는 브릿지
```

`KafkaBridge`는 라인마다 Modbus TCP에 접속해서 1초 간격으로 Input Register(화력/몰드
실측온도, 생산개수)를 읽어 `factory.modbus`에, 임베디드 브로커의 `factory/+/roomEnv`
와일드카드를 구독해서 받은 실내온습도 메시지를 그대로 `factory.roomenv`에 발행한다. 두
토픽 모두 메시지 키는 `line1`~`line7`이다.

### 확인

- 브라우저로 `http://localhost:8080` (Kafka UI) 접속 → Topics 메뉴에서 `factory.modbus`,
  `factory.roomenv` 두 토픽을 연다. Messages 탭에서 key=line1~line7 메시지가 계속
  새로 들어오는지, 값(화력/몰드온도, 생산개수, 온습도)이 시간에 따라 자연스럽게
  바뀌는지 확인한다.
- 콘솔로 교차 확인하려면:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic factory.modbus --from-beginning

docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic factory.roomenv --from-beginning
```

> **범위 참고:** `index.html`(물리 시뮬레이션이 붙은 전체 3D 데모)과 TimescaleDB 적재는
> 아직 범위 밖이다. Kafka Streams 정규화 / WebSocket 릴레이는 아래 절에서 다룬다.

### 정규화(Kafka Streams) + 실시간 WebSocket 릴레이

`factory.modbus`(1초마다 확정 발행되는 스트림)와 `factory.roomenv`(그때그때 온 최신값을
유지하는 테이블)를 라인ID 기준으로 join해서, 하나의 통일된 스키마로 `factory.linestate`에
다시 쓰는 Kafka Streams 앱과, 그 결과를 저장 없이 곧바로 브라우저로 relay하는 WebSocket
백엔드가 있다.

```bash
./gradlew runLineStateStreams      # 터미널 3: factory.modbus + factory.roomenv -> factory.linestate
./gradlew runLiveWebSocketServer   # 터미널 4: factory.linestate -> WebSocket(8081)으로 브라우저에 relay
```

- `factory.linestate` 메시지 예: `{"lineId":"line1","fireActual":220.0,"moldActual":200.3,"servedCount":5225,"roomTemp":21.1,"roomHumidity":40.5,"ts":...}`
- 이 뒤로 이어지는 두 갈래(콜드 패스: DB sink → TimescaleDB / 핫 패스: 이 WebSocket → 화면)
  중 지금은 핫 패스만 구현돼 있다. DB 적재는 여전히 범위 밖.
- 브라우저에서 `src/main/java/com/factory/sim/live-monitor.html`을 열면(파일 직접 열기,
  `ws://localhost:8081`에 접속) 라인별 카드에 화력/몰드온도, 실내온습도, 생산개수가
  실시간으로 계속 갱신되는 걸 확인할 수 있다. 최소한의 Three.js 뷰어로, 데이터가 도착할
  때마다 카드가 잠깐 강조됐다가 원래대로 돌아온다.

## 사용한 라이브러리

| 용도 | 라이브러리 | 버전 |
|---|---|---|
| Modbus TCP 서버/클라이언트 | `com.ghgande:j2mod` | 3.3.0 |
| MQTT 클라이언트 | `org.eclipse.paho:org.eclipse.paho.client.mqttv3` | 1.2.5 |
| 임베디드 MQTT 브로커 | `io.moquette:moquette-broker` | 0.17 |
| Kafka producer (KafkaBridge용) | `org.apache.kafka:kafka-clients` | 3.8.0 |
| Kafka Streams (정규화 조인용) | `org.apache.kafka:kafka-streams` | 3.8.0 |
| WebSocket 서버 (실시간 relay용) | `org.java-websocket:Java-WebSocket` | 1.5.6 |
