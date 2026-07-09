package com.factory.sim.modbus;

import com.ghgande.j2mod.modbus.procimg.Register;

import java.util.function.IntConsumer;
import java.util.function.IntSupplier;

/**
 * "읽기 + 쓰기 가능" Modbus 홀딩 레지스터 어댑터 (Holding Register, FC=0x03 읽기 / FC=0x06 쓰기 용).
 *
 * <p>{@link DelegatingInputRegister}와 원리는 같은데, 여기서는 쓰기(setValue)도 지원해야 한다.
 * Modbus 마스터가 FC=0x06(단일 레지스터 쓰기)으로 값을 보내면 j2mod가 이 객체의
 * {@code setValue(...)}를 호출하고, 우리는 그 값을 그대로 {@link FactoryState}의 setter로
 * 전달(delegate)한다. 즉 이 클래스는 값을 저장하지 않는 "통로"이며, 실제 값의 유일한 보관소는
 * FactoryState뿐이다.</p>
 */
public class DelegatingRegister implements Register {

    private final IntSupplier valueSupplier;
    private final IntConsumer valueConsumer;

    public DelegatingRegister(IntSupplier valueSupplier, IntConsumer valueConsumer) {
        this.valueSupplier = valueSupplier;
        this.valueConsumer = valueConsumer;
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

    @Override
    public void setValue(int value) {
        valueConsumer.accept(value);
    }

    @Override
    public void setValue(short value) {
        valueConsumer.accept(value);
    }

    @Override
    public void setValue(byte[] bytes) {
        int value = ((bytes[0] & 0xFF) << 8) | (bytes[1] & 0xFF);
        valueConsumer.accept(value);
    }
}
