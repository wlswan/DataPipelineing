package com.factory.sim.modbus;

import com.ghgande.j2mod.modbus.procimg.InputRegister;

import java.util.function.IntSupplier;

/**
 * "읽기 전용" Modbus 레지스터 어댑터 (Input Register, FC=0x04 용).
 *
 * <p>j2mod 라이브러리는 레지스터 하나하나를 {@link InputRegister} 인터페이스로 추상화한다.
 * j2mod가 제공하는 {@code SimpleInputRegister}를 쓰면 값을 그 안에 "복사"해서 들고 있어야 하는데,
 * 그렇게 하면 물리값을 어딘가 두 군데(FactoryState + 레지스터 객체)에 나눠 저장하는 꼴이 되어
 * "물리 상태는 한 곳에서만 계산한다"는 요구사항이 깨진다.</p>
 *
 * <p>그래서 값을 직접 들고 있지 않고, Modbus 마스터가 값을 물어볼 때마다({@code getValue()} 호출 시)
 * {@link FactoryState}의 getter를 즉시 호출해서 최신 값을 그대로 돌려주는 "얇은 통로(delegate)"
 * 역할만 하는 클래스를 만들었다. 생성자로 {@code IntSupplier}(인자 없이 int를 반환하는 함수)를
 * 받아서, 예를 들어 {@code state::getIrTempX10} 처럼 FactoryState의 getter 메서드 참조를
 * 그대로 넘겨 쓸 수 있다.</p>
 */
public class DelegatingInputRegister implements InputRegister {

    private final IntSupplier valueSupplier;

    public DelegatingInputRegister(IntSupplier valueSupplier) {
        this.valueSupplier = valueSupplier;
    }

    @Override
    public int getValue() {
        return valueSupplier.getAsInt();
    }

    @Override
    public int toUnsignedShort() {
        return getValue() & 0xFFFF;
    }

    @Override
    public short toShort() {
        return (short) getValue();
    }

    @Override
    public byte[] toBytes() {
        int unsignedValue = toUnsignedShort();
        return new byte[]{
                (byte) ((unsignedValue >> 8) & 0xFF),
                (byte) (unsignedValue & 0xFF)
        };
    }
}
