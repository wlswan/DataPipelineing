package com.factory.sim.streams;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 이 프로젝트의 다른 클래스들(KafkaBridge, RoomEnvPublisher)이 JSON을 만들 때 외부 라이브러리
 * 없이 문자열로 직접 조립하는 것과 마찬가지로, 여기서도 필드가 몇 개뿐인 평평한(flat) JSON을
 * 파싱하기 위해 별도 JSON 라이브러리 없이 정규식으로 값만 뽑아낸다.
 *
 * <p>{@code factory.linestate}를 소비하는 쪽(스트림 join, DB sink)이 모두 같은 스키마를
 * 파싱해야 해서 public으로 열어 공유한다.</p>
 */
public final class JsonFields {

    private JsonFields() {
    }

    public static String str(String json, String field) {
        Matcher m = Pattern.compile("\"" + field + "\":\"([^\"]*)\"").matcher(json);
        return m.find() ? m.group(1) : null;
    }

    public static double num(String json, String field, double defaultValue) {
        if (json == null) {
            return defaultValue;
        }
        Matcher m = Pattern.compile("\"" + field + "\":(-?\\d+\\.?\\d*)").matcher(json);
        return m.find() ? Double.parseDouble(m.group(1)) : defaultValue;
    }
}
