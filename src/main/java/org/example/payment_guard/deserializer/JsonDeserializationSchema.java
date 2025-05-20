package org.example.payment_guard.deserializer;

import com.example.test1.entity.MenuItem;
import com.example.test1.entity.ReceiptData;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * JSON 형식의 데이터를 ReceiptData 객체로 역직렬화하는 DeserializationSchema 구현체입니다.
 */
public class JsonDeserializationSchema implements DeserializationSchema<ReceiptData> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public ReceiptData deserialize(byte[] message) throws IOException {
        try {
            // 테스트를 위해 입력 데이터 로그 추가
            System.out.println("\n----- 수신된 JSON 데이터 정보 -----");
            System.out.println("Data length: " + (message != null ? message.length : "null"));
            
            // 이진 데이터의 처음 부분 확인
            if (message != null && message.length > 0) {
                StringBuilder hexDump = new StringBuilder();
                for (int i = 0; i < Math.min(20, message.length); i++) {
                    hexDump.append(String.format("%02X ", message[i] & 0xFF));
                }
                System.out.println("Binary data (hex): " + hexDump.toString());
            }
            
            // 다양한 인코딩 형식 시도
            String jsonStr;
            try {
                jsonStr = new String(message, "UTF-8");
                // 긴 경우 일부만 출력
                System.out.println("UTF-8 decoded: " + (jsonStr.length() > 200 ? jsonStr.substring(0, 200) + "..." : jsonStr));
            } catch (Exception e) {
                // UTF-8 디코딩 실패시 자동적으로 fallback 처리
                System.err.println("UTF-8 디코딩 오류: " + e.getMessage());
                // 오류 발생 시 더미 데이터로 대체
                ReceiptData fallbackData = createFallbackData();
                return fallbackData;
            }
            
            // JSON 문자열을 JsonNode로 파싱
            try {
                JsonNode rootNode = objectMapper.readTree(jsonStr);
                ReceiptData receiptData = extractDataFromJson(rootNode);
                return receiptData;
            } catch (Exception e) {
                System.err.println("JSON 파싱 중 오류: " + e.getMessage());
                
                // JSON 파싱 실패 시 UTF-8 문자열에서 패턴 기반 추출 시도
                try {
                    ReceiptData parsedData = extractDataFromText(jsonStr);
                    if (parsedData != null) {
                        // 중요: menu_items가 null이 아닌지 마지막으로 확인
                        if (parsedData.getMenuItems() == null) {
                            parsedData.setMenuItems(new ArrayList<>());
                        }
                        System.out.println("패턴 기반 파싱 성공");
                        return parsedData;
                    }
                } catch (Exception ex) {
                    System.err.println("패턴 기반 파싱 실패: " + ex.getMessage());
                }
                
                // 모든 파싱 시도 실패 시 더미 데이터 반환
                System.err.println("모든 파싱 시도 실패. 더미 데이터 반환");
            }
            
            // 오류 발생 시 더미 데이터로 대체
            return createFallbackData();
            
        } catch (Exception e) {
            System.err.println("JSON 파싱 중 오류: " + e.getMessage());
            e.printStackTrace();
            
            // 오류 발생 시 더미 데이터로 대체
            return createFallbackData();
        }
    }
    
    /**
     * 더미 데이터 생성
     */
    private ReceiptData createFallbackData() {
        ReceiptData fallbackData = new ReceiptData();
        fallbackData.setFranchiseId(0);
        fallbackData.setStoreBrand("오류 발생");
        fallbackData.setStoreId(999);
        fallbackData.setStoreName("오류 발생 상점");
        
        // 중요: menu_items가 절대 null이 되지 않도록 반드시 빈 리스트로 초기화
        List<MenuItem> emptyList = new ArrayList<>();
        fallbackData.setMenuItems(emptyList);
        
        // 시간 정보도 추가
        fallbackData.setTime(new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date()));
        
        // region 및 주소 정보도 추가
        fallbackData.setRegion("알 수 없음");
        fallbackData.setStoreAddress("알 수 없음");
        
        // 사용자 정보
        fallbackData.setUserId(0);
        fallbackData.setUserName("알 수 없음");
        fallbackData.setUserGender("알 수 없음");
        fallbackData.setUserAge(0);
        
        return fallbackData;
    }
    
    /**
     * JsonNode에서 데이터 추출
     */
    private ReceiptData extractDataFromJson(JsonNode rootNode) {
        // 기본 값으로 초기화된 ReceiptData 객체 생성
        ReceiptData receiptData = new ReceiptData();
        
        // 중요: menu_items를 반드시 빈 리스트로 초기화
        receiptData.setMenuItems(new ArrayList<>());
        
        // 필드 값 추출 및 설정
        if (rootNode.has("franchise_id")) {
            receiptData.setFranchiseId(rootNode.get("franchise_id").asInt());
        }
        
        if (rootNode.has("store_brand")) {
            receiptData.setStoreBrand(rootNode.get("store_brand").asText());
        }
        
        if (rootNode.has("store_id")) {
            receiptData.setStoreId(rootNode.get("store_id").asInt());
        }
        
        if (rootNode.has("store_name")) {
            receiptData.setStoreName(rootNode.get("store_name").asText());
        }
        
        if (rootNode.has("region")) {
            receiptData.setRegion(rootNode.get("region").asText());
        }
        
        if (rootNode.has("store_address")) {
            receiptData.setStoreAddress(rootNode.get("store_address").asText());
        }
        
        // 메뉴 아이템 설정 - 반드시 null이 아닌 빈 리스트로 초기화
        List<MenuItem> menuItems = new ArrayList<>();
        if (rootNode.has("menu_items") && rootNode.get("menu_items").isArray()) {
            ArrayNode menuItemsNode = (ArrayNode) rootNode.get("menu_items");
            for (JsonNode menuItemNode : menuItemsNode) {
                MenuItem menuItem = new MenuItem();
                
                if (menuItemNode.has("menu_id")) {
                    menuItem.setMenuId(menuItemNode.get("menu_id").asInt());
                }
                
                if (menuItemNode.has("menu_name")) {
                    menuItem.setMenuName(menuItemNode.get("menu_name").asText());
                }
                
                if (menuItemNode.has("unit_price")) {
                    menuItem.setUnitPrice(menuItemNode.get("unit_price").asInt());
                }
                
                if (menuItemNode.has("quantity")) {
                    menuItem.setQuantity(menuItemNode.get("quantity").asInt());
                }
                
                menuItems.add(menuItem);
            }
        }
        // 어떤 경우도 menuItems를 null로 설정하지 않음
        receiptData.setMenuItems(menuItems);
        
        if (rootNode.has("total_price")) {
            receiptData.setTotalPrice(rootNode.get("total_price").asInt());
        }
        
        if (rootNode.has("user_id")) {
            receiptData.setUserId(rootNode.get("user_id").asInt());
        }
        
        if (rootNode.has("time")) {
            receiptData.setTime(rootNode.get("time").asText());
        }
        
        if (rootNode.has("user_name")) {
            receiptData.setUserName(rootNode.get("user_name").asText());
        }
        
        if (rootNode.has("user_gender")) {
            receiptData.setUserGender(rootNode.get("user_gender").asText());
        }
        
        if (rootNode.has("user_age")) {
            receiptData.setUserAge(rootNode.get("user_age").asInt());
        }
        
        // 파싱 결과 출력
        System.out.println("JSON 파싱 완료: store_id=" + receiptData.getStoreId() + 
                      ", store_brand=" + receiptData.getStoreBrand() + 
                      ", store_name=" + receiptData.getStoreName());
        
        return receiptData;
    }
    
    /**
     * 텍스트 데이터에서 패턴 매칭을 통해 데이터 추출
     */
    private ReceiptData extractDataFromText(String text) {
        if (text == null || text.isEmpty()) {
            return null;
        }
        
        // 기본 값으로 초기화된 ReceiptData 객체 생성
        ReceiptData receiptData = new ReceiptData();
        
        // 중요: menu_items를 반드시 빈 리스트로 초기화
        receiptData.setMenuItems(new ArrayList<>());
        
        // 스토어 정보 추출 시도
        try {
            // 스토어 브랜드 추출
            java.util.regex.Pattern brandPattern = java.util.regex.Pattern.compile("([\\uac00-\\ud7a3\\w]+)\\s*[\\uac1c부]");
            java.util.regex.Matcher brandMatcher = brandPattern.matcher(text);
            if (brandMatcher.find()) {
                receiptData.setStoreBrand(brandMatcher.group(1));
            } else {
                // 식당 이름 패턴 (간단한 한글 식당명)
                String[] knownBrands = {"한신포차", "대한국밥", "돌배기집", "롤링파스타", "리춤시장", "극단이오름",
                                       "미정국수0410", "백스비어", "본가", "빵다방", "빽보이피자", "새마을식당",
                                       "역전우동0410", "연돈볼카츠", "원조쌈밥집", "인생설렁탕", "재순식당", "홍콩반점0410", "고투웍"};
                for (String brand : knownBrands) {
                    if (text.contains(brand)) {
                        receiptData.setStoreBrand(brand);
                        break;
                    }
                }
            }
            
            // 스토어 이름 추출 (기본 구조: 브랜드 + 지점명)
            java.util.regex.Pattern storePattern = java.util.regex.Pattern.compile("([\\uac00-\\ud7a3\\w]+\\s*점)");
            java.util.regex.Matcher storeMatcher = storePattern.matcher(text);
            if (storeMatcher.find()) {
                receiptData.setStoreName(storeMatcher.group(1));
            }
            
            // 총 가격 추출
            java.util.regex.Pattern pricePattern = java.util.regex.Pattern.compile("\\b(\\d{3,6})\\b");
            java.util.regex.Matcher priceMatcher = pricePattern.matcher(text);
            if (priceMatcher.find()) {
                try {
                    receiptData.setTotalPrice(Integer.parseInt(priceMatcher.group(1)));
                } catch (NumberFormatException e) {
                    // 무시
                }
            }
            
            // 날짜/시간 추출
            java.util.regex.Pattern timePattern = java.util.regex.Pattern.compile("(\\d{4}-\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}:\\d{2})");
            java.util.regex.Matcher timeMatcher = timePattern.matcher(text);
            if (timeMatcher.find()) {
                receiptData.setTime(timeMatcher.group(1));
            } else {
                // 시간 정보가 없으면 현재 시간으로 설정
                receiptData.setTime(new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date()));
            }
            
            // 임의의 스토어 ID 설정
            if (receiptData.getStoreBrand() != null && !receiptData.getStoreBrand().isEmpty()) {
                // 브랜드 이름을 기반으로 임의 ID 생성
                receiptData.setStoreId(Math.abs(receiptData.getStoreBrand().hashCode() % 50) + 1);
            } else {
                receiptData.setStoreBrand("알 수 없음");
                receiptData.setStoreId(999);
            }
            
            if (receiptData.getStoreName() == null || receiptData.getStoreName().isEmpty()) {
                receiptData.setStoreName("알 수 없는 지점");
            }
            
            // 사용자 정보 추출
            java.util.regex.Pattern namePattern = java.util.regex.Pattern.compile("([\\uac00-\\ud7a3]{2,4})");
            java.util.regex.Matcher nameMatcher = namePattern.matcher(text);
            if (nameMatcher.find()) {
                receiptData.setUserName(nameMatcher.group(1));
            } else {
                receiptData.setUserName("알 수 없음");
            }
            
            // 임의의 사용자 ID 설정
            receiptData.setUserId(10000 + (int)(Math.random() * 90000)); // 10000-99999 사이의 난수
            
            // region 정보 설정
            receiptData.setRegion("알 수 없음");
            
            // 필요한 초기값 설정
            if (receiptData.getUserGender() == null) {
                receiptData.setUserGender("알 수 없음");
            }
            
            System.out.println("패턴 기반 데이터 추출: store_id=" + receiptData.getStoreId() + 
                           ", store_brand=" + receiptData.getStoreBrand() + 
                           ", store_name=" + receiptData.getStoreName());
            
            return receiptData;
        } catch (Exception e) {
            System.err.println("패턴 기반 추출 오류: " + e.getMessage());
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(ReceiptData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<ReceiptData> getProducedType() {
        return TypeInformation.of(ReceiptData.class);
    }
}