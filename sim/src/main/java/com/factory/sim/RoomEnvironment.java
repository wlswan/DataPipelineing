package com.factory.sim;

import java.util.concurrent.atomic.AtomicReference;

/**
 * 공장 전체가 공유하는 실내 온습도 상태.
 *
 * <p>실내온습도 센서는 라인마다 있는 게 아니라 건물 전체에 하나만 있다고 가정한다. 그래서
 * 라인별로 따로 두는 {@link FactoryState}와 달리, 이 객체는 프로세스 전체에서 딱 하나만
 * 만들어지고 모든 라인의 {@link PhysicsSimulator}와 단 하나뿐인
 * {@link com.factory.sim.mqtt.RoomEnvPublisher}가 함께 들여다본다.</p>
 *
 * <p>값의 흐름 규칙은 {@link FactoryState}와 동일하다: {@link RoomEnvSimulator} 딱 한 곳에서만
 * 쓰기(write)가 일어나고, 나머지(PhysicsSimulator의 목표 몰드온도 계산, MQTT 퍼블리셔)는
 * 읽기(read)만 한다.</p>
 */
public final class RoomEnvironment {

    private final AtomicReference<RoomEnv> current = new AtomicReference<>(new RoomEnv(22.0, 50.0));

    public RoomEnv get() {
        return current.get();
    }

    void set(double temp, double humidity) {
        current.set(new RoomEnv(temp, humidity));
    }

    /**
     * 실내 온습도를 담는 불변(immutable) 값 객체.
     * 두 값(temp, humidity)을 항상 "짝"으로 함께 교체해야 하기 때문에,
     * 필드 두 개를 따로 두지 않고 이렇게 하나로 묶었다.
     */
    public static final class RoomEnv {
        public final double temp;
        public final double humidity;

        public RoomEnv(double temp, double humidity) {
            this.temp = temp;
            this.humidity = humidity;
        }
    }
}
