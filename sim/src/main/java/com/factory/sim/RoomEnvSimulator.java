package com.factory.sim;

import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 공장 전체가 공유하는 실내온습도를 시뮬레이션하는 전용 스레드.
 *
 * <p>실제 온습도 센서는 라인마다 있는 게 아니라 건물에 하나만 있으므로, {@link PhysicsSimulator}와
 * 달리 라인 개수만큼 뜨지 않고 프로세스 전체에서 단 하나만 떠서 {@link RoomEnvironment} 하나를
 * 갱신한다. 모든 라인의 PhysicsSimulator는 이 값을 목표 몰드온도 계산에 읽기만 한다.</p>
 */
public final class RoomEnvSimulator implements Runnable {

    /** 물리 계산 주기 (초). PhysicsSimulator와 동일한 주기로 갱신한다. */
    private static final double DT_SECONDS = 0.5;

    private final RoomEnvironment environment;
    private final Random random = new Random();

    private double roomTemp = 22.0;
    private double roomHumidity = 50.0;

    private final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1, runnable -> {
        Thread thread = new Thread(runnable, "room-env-sim");
        thread.setDaemon(true);
        return thread;
    });

    public RoomEnvSimulator(RoomEnvironment environment) {
        this.environment = environment;
    }

    /** 0.5초 간격으로 {@link #run()}을 반복 호출하도록 예약한다. */
    public void start() {
        long periodMillis = (long) (DT_SECONDS * 1000);
        executor.scheduleAtFixedRate(this, 0, periodMillis, TimeUnit.MILLISECONDS);
    }

    /** 시뮬레이션 스레드를 정지시킨다. */
    public void stop() {
        executor.shutdownNow();
    }

    @Override
    public void run() {
        try {
            tick();
        } catch (RuntimeException e) {
            e.printStackTrace();
        }
    }

    /** 실내온습도는 사람이 문을 여닫거나 날씨가 바뀌는 정도의 "아주 느린 랜덤워크"로 흔들린다. */
    private void tick() {
        roomTemp += (random.nextDouble() - 0.5) * 0.02;
        roomHumidity += (random.nextDouble() - 0.5) * 0.05;
        roomHumidity = Math.max(0.0, Math.min(100.0, roomHumidity));
        environment.set(roomTemp, roomHumidity);
    }
}
