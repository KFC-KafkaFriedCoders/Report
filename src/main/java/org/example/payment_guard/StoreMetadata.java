package org.example.payment_guard;

import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;
import java.util.List;

public class StoreMetadata {
    // 브랜드 목록 정의
    private static final List<String> KNOWN_BRANDS = Arrays.asList(
        "한신포차", "대한국밥", "돌배기집", "롤링파스타", "리춘시장", "막이오름", 
        "미정국수0410", "백스비어", "본가", "빽다방", "빽보이피자", "새마을식당", 
        "역전우동0410", "연돈볼카츠", "원조쌈밥집", "인생설렁탕", "재순식당", 
        "홍콩반점0410", "고투웍"
    );
    
    public static Map<Integer, String[]> getStoreMap() {
        Map<Integer, String[]> map = new HashMap<>();
        map.put(0, new String[]{"알 수 없는 브랜드", "알 수 없는 상점"});  // 기본값 개선
        
        // 한신포차 상점들
        map.put(1, new String[]{"한신포차", "시흥능곡점"});
        map.put(2, new String[]{"한신포차", "검단아라점"});
        map.put(3, new String[]{"한신포차", "한화생명볼파크점"});
        map.put(4, new String[]{"한신포차", "강남점"});
        map.put(5, new String[]{"한신포차", "서초점"});
        map.put(6, new String[]{"한신포차", "역삼점"});
        map.put(7, new String[]{"한신포차", "이태원점"});
        map.put(8, new String[]{"한신포차", "이태원마트점"});
        map.put(9, new String[]{"한신포차", "이마트영포점"});
        map.put(10, new String[]{"한신포차", "중계점"});
        map.put(11, new String[]{"한신포차", "근수역점"});
        map.put(12, new String[]{"한신포차", "사당점"});
        map.put(13, new String[]{"한신포차", "하후역점"});
        map.put(14, new String[]{"한신포차", "변형프로젝트점"});
        map.put(15, new String[]{"한신포차", "웃는떰지점"});
        map.put(16, new String[]{"한신포차", "워서점"});
        map.put(17, new String[]{"한신포차", "비전시티점"});
        map.put(18, new String[]{"한신포차", "홍대점"});
        
        // 다른 브랜드 상점들 추가
        map.put(20, new String[]{"대한국밥", "본점"});
        map.put(21, new String[]{"돌배기집", "강남점"});
        map.put(22, new String[]{"롤링파스타", "홍대점"});
        map.put(23, new String[]{"리춘시장", "강남점"});
        map.put(24, new String[]{"막이오름", "삼성점"});
        map.put(25, new String[]{"미정국수0410", "본점"});
        map.put(26, new String[]{"백스비어", "강남점"});
        map.put(27, new String[]{"본가", "역삼점"});
        map.put(28, new String[]{"빽다방", "종로점"});
        map.put(29, new String[]{"빽보이피자", "영등포점"});
        map.put(30, new String[]{"새마을식당", "홍대점"});
        map.put(31, new String[]{"역전우동0410", "강남점"});
        map.put(32, new String[]{"연돈볼카츠", "용산점"});
        map.put(33, new String[]{"원조쌈밥집", "종로점"});
        map.put(34, new String[]{"인생설렁탕", "을지로점"});
        map.put(35, new String[]{"재순식당", "강남점"});
        map.put(36, new String[]{"홍콩반점0410", "을지로점"});
        map.put(37, new String[]{"고투웍", "선릉점"});
        
        return map;
    }
    
    // 상점 ID로 메타데이터를 가져오는 메서드, 존재하지 않는 ID는 브랜드 기반으로 추정
    public static String[] getStoreInfo(int storeId) {
        Map<Integer, String[]> storeMap = getStoreMap();
        String[] storeInfo = storeMap.get(storeId);
        
        // 해당 ID가 없으면 브랜드와 임의 지점명 생성
        if (storeInfo == null) {
            // 추정된 브랜드와 점포명을 반환
            String brandName = inferBrandName(storeId);
            String storeName = getRandomLocationFor(brandName);
            return new String[]{brandName, storeName};
        }
        
        return storeInfo;
    }
    
    // 상점 ID를 기반으로 브랜드를 추정하는 메서드
    private static String inferBrandName(int storeId) {
        // ID의 첫 자리수에 따라 분류 (마지막 두 자리만 사용)
        int brandIndex = Math.abs(storeId % KNOWN_BRANDS.size());
        return KNOWN_BRANDS.get(brandIndex);
    }
    
    // 브랜드명이 알려진 브랜드 목록에 있는지 확인
    public static boolean isKnownBrand(String brandName) {
        if (brandName == null) return false;
        return KNOWN_BRANDS.contains(brandName);
    }
    
    // 브랜드에 따른 랜덤 지점명 반환
    private static String getRandomLocationFor(String brandName) {
        // 브랜드별 주요 입점 지역
        String[] locations = {
            "강남", "서초", "잠실", "홍대", "신촌", "종로", "명동", "을지로", 
            "성수", "이태원", "역삼", "대학로", "구의", "연남", "선릉", "안국", "삼성"
        };
        
        int locationIndex = Math.abs(brandName.hashCode() % locations.length);
        return locations[locationIndex] + "점";
    }
}