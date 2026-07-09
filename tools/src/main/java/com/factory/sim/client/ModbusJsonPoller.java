package com.factory.sim.client;

import com.ghgande.j2mod.modbus.facade.ModbusTCPMaster;
import com.ghgande.j2mod.modbus.procimg.InputRegister;

import java.util.Locale;

/**
 * Modbus Input Register(화력/몰드 실측온도, 생산개수 누적)를 한 번 읽어서 JSON 한 줄을
 * 표준출력에 찍고 종료하는 커맨드라인 도구.
 * <p>NiFi 1.x는 Modbus 프로세서를 기본 제공하지 않는다. 대신 NiFi가 표준으로 갖고 있는
 * {@code ExecuteStreamCommand} 프로세서가 이 클래스를 주기적으로 실행해서, stdout으로
 * 나온 JSON을 flowfile 내용으로 받아가는 방식으로 Modbus 폴링을 흉내낸다. 즉 이 클래스는
 * "NiFi가 없는 Modbus 폴링 기능을 대신 수행해주는 아주 작은 브릿지 프로그램"이다.</p>
 *
 * <p>실행: {@code java -cp <classpath> com.factory.sim.client.ModbusJsonPoller <host> <port> <lineId>}</p>
 *
 * <p>출력 예시: {@code {"lineId":3,"fireActual":220.1,"moldActual":200.3,"servedCount":12}}</p>
 *
 * <p>접속/읽기에 실패하면 표준에러에 메시지를 남기고 0이 아닌 종료 코드로 끝난다 —
 * {@code ExecuteStreamCommand}가 이걸 실패(failure) 관계로 라우팅할 수 있게 하기 위함이다.</p>
 */
public final class ModbusJsonPoller {

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("사용법: ModbusJsonPoller <host> <port> <lineId>");
            System.exit(1);
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        int lineId = Integer.parseInt(args[2]);

        ModbusTCPMaster master = new ModbusTCPMaster(host, port);
        try {
            master.connect();
            InputRegister[] regs = master.readInputRegisters(0, 3);

            double fireActual = regs[0].getValue() / 10.0;
            double moldActual = regs[1].getValue() / 10.0;
            int servedCount = regs[2].getValue();

            String json = String.format(
                    Locale.US,
                    "{\"lineId\":%d,\"fireActual\":%.1f,\"moldActual\":%.1f,\"servedCount\":%d}",
                    lineId, fireActual, moldActual, servedCount);

            System.out.println(json);
        } catch (Exception e) {
            System.err.println("Modbus 읽기 실패 (host=" + host + ", port=" + port + "): " + e.getMessage());
            System.exit(1);
        } finally {
            master.disconnect();
        }
    }
}
