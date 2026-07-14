package com.factory.sim;

import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 물리 시뮬레이션 스레드.
 *
 * <p>자바에서 "일정 주기로 반복 실행"을 하고 싶을 때 가장 쉬운 방법은
 * {@link ScheduledExecutorService}를 쓰는 것이다. {@code new Thread()} + {@code while(true) + sleep()}
 * 로 직접 짜는 것보다 예외 처리, 종료(shutdown) 처리가 훨씬 간단하다.</p>
 *
 * <p>이 클래스는 {@link FactoryState}의 "센서값"을 갱신하는 유일한 주체다. 다른 어떤 클래스도
 * 센서값을 직접 쓰지 않는다 (setter가 패키지 전용이라 애초에 불가능하다). 이렇게 "한 곳에서만
 * 쓰기가 일어난다"는 규칙을 지키면, 여러 스레드가 얽혀도 값이 꼬일 걱정을 크게 줄일 수 있다.</p>
 *
 * <p>적외선 온도계로 재는 실측온도는 더 이상 외부 제어값(SV)을 따라가는 게 아니라, 그
 * 자체로 아주 느린 랜덤워크로 흔들리는 순수 측정값이다 - 라인마다 독립적으로 흔들린다는
 * 점만 {@link RoomEnvSimulator}(공장 전체 공유)와 다르다.</p>
 */
public final class PhysicsSimulator implements Runnable {

    /** 물리 계산 주기 (초). 문제에서 요구한 "0.5초마다 갱신"에 대응. */
    private static final double DT_SECONDS = 0.5;

    /** 센서 오차(노이즈)의 최대 폭. 문제에서 요구한 "±0.3 정도"에 대응. */
    private static final double SENSOR_NOISE_RANGE = 0.3;

    private final FactoryState state;
    private final Random random = new Random();

    /**
     * 물리 계산 전용 스레드를 만들어주는 실행기(executor).
     * daemon 스레드로 만들어서, 메인 스레드가 끝나면 JVM이 이 스레드 때문에 안 죽고 잘 종료되게 한다.
     */
    private final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1, runnable -> {
        Thread thread = new Thread(runnable, "physics-sim");
        thread.setDaemon(true);
        return thread;
    });

    // ---- 물리 계산용 내부 상태 (double로 고정밀 유지) ----
    // FactoryState에는 x10 스케일 정수(클램핑됨)로만 내보내고, 계산 자체는 이 필드로 한다.
    private double irTemp;    // 적외선 온도계로 측정하는 실측온도 (°C)

    public PhysicsSimulator(FactoryState state) {
        this.state = state;
        // FactoryState의 초기값과 어긋나지 않도록 내부 상태도 같은 값에서 시작한다.
        this.irTemp = state.getIrTempX10() / 10.0;
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
            // scheduleAtFixedRate는 실행 중 예외가 던져지면 이후 반복 자체를 멈춰버린다.
            // 시뮬레이터가 조용히 멈추는 것보다는, 에러를 로그로 남기고 다음 tick도 계속 도는 게 낫다.
            e.printStackTrace();
        }
    }

    /** 한 번의 물리 계산 스텝. 0.5초마다 이 메서드가 호출된다. */
    private void tick() {
        // 1) 적외선 온도계 실측온도는 사람이 문을 여닫거나 공정이 미세하게 바뀌는 정도의
        //    "아주 느린 랜덤워크"로 흔들린다 (실내온습도와 같은 방식, 다만 라인마다 독립적).
        irTemp += (random.nextDouble() - 0.5) * 0.05;

        // 2) 실측값에는 센서 오차를 흉내낸 작은 랜덤 노이즈(±0.3)를 더한다.
        double irNoisy = irTemp + (random.nextDouble() - 0.5) * (2 * SENSOR_NOISE_RANGE);

        // 3) 계산 결과를 공유 상태(FactoryState)에 반영한다.
        //    x10 스케일 정수 변환과 16비트 범위 클램핑은 FactoryState 내부에서 처리한다.
        state.setIrTempX10((int) Math.round(irNoisy * 10));
    }
}
