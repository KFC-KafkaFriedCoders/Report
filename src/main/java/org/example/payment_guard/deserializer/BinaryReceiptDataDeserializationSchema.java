package org.example.payment_guard.deserializer;

import com.example.test1.entity.ReceiptData;
import com.example.test1.entity.MenuItem;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.example.payment_guard.StoreMetadata;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

/**
 * 바이너리 형식으로 된 ReceiptData를 직접 파싱하는 DeserializationSchema 구현체입니다.
 */
public class BinaryReceiptDataDeserializationSchema implements DeserializationSchema<ReceiptData> {

    // 알려진 브랜드명 목록
    private static final List<String> KNOWN_BRANDS = Arrays.asList(
        "한신포차", "대한국밥", "돌배기집", "롤링파스타", "리춘시장", "막이오름", 
        "미정국수0410", "백스비어", "본가", "빽다방", "빽보이피자", "새마을식당", 
        "역전우동0410", "연돈볼카츠", "원조쌈밥집", "인생설렁탕", "재순식당", 
        "홍콩반점0410", "고투웍"
    );
    
    // 지역명 목록 (이는 브랜드가 아닌 주소에 해당함)
    private static final List<String> LOCATION_TERMS = Arrays.asList(
        "송파구", "강남구", "종로구", "중구", "서대문구", "마포구", "노원구", "광진구",
        "영등포구", "동작구", "서울", "경기", "인천", "부산", "대구", "대전",
        "광주", "울산", "세종", "충북", "충남", "경북", "경남", "전북", "전남", "제주", "강원"
    );

    @Override
    public ReceiptData deserialize(byte[] message) throws IOException {
        try {
            // 기본 값으로 초기화된 ReceiptData 객체 생성
            ReceiptData receiptData = new ReceiptData();
            receiptData.setFranchiseId(0); 
            receiptData.setStoreBrand(null); // null로 설정 (스키마 변경 후)
            receiptData.setStoreId(0);
            receiptData.setStoreName(null);
            receiptData.setRegion(null);
            receiptData.setStoreAddress(null);
            receiptData.setMenuItems(new ArrayList<>());
            receiptData.setTotalPrice(0);
            receiptData.setUserId(0);
            receiptData.setTime(null);
            receiptData.setUserName(null);
            receiptData.setUserGender(null);
            receiptData.setUserAge(0);
            
            if (message == null || message.length == 0) {
                System.out.println("입력 메시지가 비어있습니다. 기본 객체를 반환합니다.");
                return receiptData;
            }
            
            ByteBuffer buffer = ByteBuffer.wrap(message);
            
            // franchise_id가 존재하면 읽기
            if (hasMoreData(buffer, 4)) {
                receiptData.setFranchiseId(buffer.getInt());
            }
            
            // store_brand 읽기
            if (hasMoreData(buffer, 4)) {
                int stringLen = readStringLength(buffer);
                if (stringLen > 0 && hasMoreData(buffer, stringLen)) {
                    byte[] bytes = new byte[stringLen];
                    buffer.get(bytes);
                    String storeBrand = new String(bytes, "UTF-8");
                    
                    // 브랜드명 보정
                    storeBrand = correctBrandName(storeBrand);
                    receiptData.setStoreBrand(storeBrand);
                }
            }
            
            // store_id 읽기
            if (hasMoreData(buffer, 4)) {
                int storeId = buffer.getInt();
                
                // 음수 값이 나오면 로그 출력
                if (storeId < 0) {
                    System.out.println("주의: 음수 store_id 발견: " + storeId);
                    // 음수 방지를 위해 절대값 사용
                    storeId = Math.abs(storeId);
                }
                
                receiptData.setStoreId(storeId);
            }
            
            // store_name 읽기
            if (hasMoreData(buffer, 4)) {
                int stringLen = readStringLength(buffer);
                if (stringLen > 0 && hasMoreData(buffer, stringLen)) {
                    byte[] bytes = new byte[stringLen];
                    buffer.get(bytes);
                    String storeName = new String(bytes, "UTF-8");
                    receiptData.setStoreName(storeName); // null 허용
                }
            }
            
            // region 읽기
            if (hasMoreData(buffer, 4)) {
                int stringLen = readStringLength(buffer);
                if (stringLen > 0 && hasMoreData(buffer, stringLen)) {
                    byte[] bytes = new byte[stringLen];
                    buffer.get(bytes);
                    String region = new String(bytes, "UTF-8");
                    receiptData.setRegion(region); // null 허용
                }
            }
            
            // store_address 읽기
            if (hasMoreData(buffer, 4)) {
                int stringLen = readStringLength(buffer);
                if (stringLen > 0 && hasMoreData(buffer, stringLen)) {
                    byte[] bytes = new byte[stringLen];
                    buffer.get(bytes);
                    String storeAddress = new String(bytes, "UTF-8");
                    receiptData.setStoreAddress(storeAddress); // null 허용
                }
            }
            
            // menu_items 읽기
            List<MenuItem> menuItems = new ArrayList<>();
            if (hasMoreData(buffer, 4)) {
                int arrayLength = buffer.getInt();
                if (arrayLength > 0) {
                    for (int i = 0; i < arrayLength && hasMoreData(buffer, 4); i++) {
                        MenuItem item = new MenuItem();
                        
                        // menu_id
                        if (hasMoreData(buffer, 4)) {
                            item.setMenuId(buffer.getInt());
                        }
                        
                        // menu_name
                        if (hasMoreData(buffer, 4)) {
                            int menuNameLen = readStringLength(buffer);
                            if (menuNameLen > 0 && hasMoreData(buffer, menuNameLen)) {
                                byte[] bytes = new byte[menuNameLen];
                                buffer.get(bytes);
                                String menuName = new String(bytes, "UTF-8");
                                item.setMenuName(menuName); // null 허용
                            } else {
                                item.setMenuName(null); // 명시적으로 null 설정
                            }
                        } else {
                            item.setMenuName(null); // 데이터가 없는 경우 명시적으로 null 설정
                        }
                        
                        // unit_price
                        if (hasMoreData(buffer, 4)) {
                            item.setUnitPrice(buffer.getInt());
                        }
                        
                        // quantity
                        if (hasMoreData(buffer, 4)) {
                            item.setQuantity(buffer.getInt());
                        }
                        
                        menuItems.add(item);
                    }
                }
            }
            receiptData.setMenuItems(menuItems);
            
            // total_price 읽기
            if (hasMoreData(buffer, 4)) {
                receiptData.setTotalPrice(buffer.getInt());
            }
            
            // user_id 읽기
            if (hasMoreData(buffer, 4)) {
                receiptData.setUserId(buffer.getInt());
            }
            
            // time 읽기
            if (hasMoreData(buffer, 4)) {
                int stringLen = readStringLength(buffer);
                if (stringLen > 0 && hasMoreData(buffer, stringLen)) {
                    byte[] bytes = new byte[stringLen];
                    buffer.get(bytes);
                    String time = new String(bytes, "UTF-8");
                    receiptData.setTime(time); // null 허용
                }
            }
            
            // user_name 읽기
            if (hasMoreData(buffer, 4)) {
                int stringLen = readStringLength(buffer);
                if (stringLen > 0 && hasMoreData(buffer, stringLen)) {
                    byte[] bytes = new byte[stringLen];
                    buffer.get(bytes);
                    String userName = new String(bytes, "UTF-8");
                    receiptData.setUserName(userName); // null 허용
                }
            }
            
            // user_gender 읽기
            if (hasMoreData(buffer, 4)) {
                int stringLen = readStringLength(buffer);
                if (stringLen > 0 && hasMoreData(buffer, stringLen)) {
                    byte[] bytes = new byte[stringLen];
                    buffer.get(bytes);
                    String userGender = new String(bytes, "UTF-8");
                    receiptData.setUserGender(userGender); // null 허용
                }
            }
            
            // user_age 읽기
            if (hasMoreData(buffer, 4)) {
                receiptData.setUserAge(buffer.getInt());
            }
            
            // 브랜드명 확인 및 보정
            fixBrandAndStoreName(receiptData);
            
            // 로그 추가
            System.out.println("역직렬화 완료: store_id=" + receiptData.getStoreId() + 
                           ", store_brand=" + receiptData.getStoreBrand() + 
                           ", store_name=" + receiptData.getStoreName());
            
            return receiptData;
        } catch (Exception e) {
            System.err.println("데이터 파싱 중 오류: " + e.getMessage());
            e.printStackTrace();
            
            // 기본 ReceiptData 객체 반환 (실패했을 때 최소한 작동할 수 있도록)
            ReceiptData fallbackData = new ReceiptData();
            fallbackData.setFranchiseId(0);
            fallbackData.setStoreBrand("한신포차"); // 기본 브랜드 설정
            fallbackData.setStoreId(999); // 오류 발생 시 명확한 ID 부여
            fallbackData.setStoreName("오류 발생 상점");
            fallbackData.setRegion(null);
            fallbackData.setStoreAddress(null);
            fallbackData.setMenuItems(new ArrayList<>());
            fallbackData.setTotalPrice(0);
            fallbackData.setUserId(0);
            fallbackData.setTime(null);
            fallbackData.setUserName(null);
            fallbackData.setUserGender(null);
            fallbackData.setUserAge(0);
            return fallbackData;
        }
    }
    
    /**
     * 브랜드명과 상점명을 확인하고 보정합니다.
     */
    private void fixBrandAndStoreName(ReceiptData data) {
        String storeBrand = data.getStoreBrand();
        
        // 브랜드명이 비어있거나 실제 브랜드가 아닌 경우 처리
        if (storeBrand == null || storeBrand.isEmpty() || isLocationTerm(storeBrand)) {
            // ID 기반으로 브랜드 추정
            String[] storeInfo = StoreMetadata.getStoreInfo(data.getStoreId());
            data.setStoreBrand(storeInfo[0]);
            
            // 상점명이 비어있는 경우에만 설정
            if (data.getStoreName() == null || data.getStoreName().isEmpty()) {
                data.setStoreName(storeInfo[1]);
            }
        } else if (!StoreMetadata.isKnownBrand(storeBrand)) {
            // 알려진 브랜드에 없는 경우, 브랜드 매칭 시도
            String correctedBrand = findClosestBrand(storeBrand);
            if (correctedBrand != null) {
                System.out.println("브랜드명 보정: '" + storeBrand + "' -> '" + correctedBrand + "'");
                data.setStoreBrand(correctedBrand);
            }
        }
    }
    
    /**
     * 비슷한 브랜드를 찾아 반환합니다.
     */
    private String findClosestBrand(String brandName) {
        if (brandName == null || brandName.isEmpty()) return null;
        
        // 각 브랜드와 비교하여 가장 유사한 것을 찾음
        for (String brand : KNOWN_BRANDS) {
            if (brandName.contains(brand) || brand.contains(brandName)) {
                return brand;
            }
        }
        
        // 기본값으로 첫 번째 브랜드 반환
        return KNOWN_BRANDS.get(0);
    }
    
    /**
     * 입력된 문자열이 지역명인지 확인합니다.
     */
    private boolean isLocationTerm(String text) {
        if (text == null || text.isEmpty()) return false;
        
        for (String location : LOCATION_TERMS) {
            if (text.contains(location)) {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * 브랜드명을 보정합니다.
     */
    private String correctBrandName(String brandName) {
        if (brandName == null || brandName.isEmpty()) return null;
        
        // 지역명이면 null 반환
        if (isLocationTerm(brandName)) {
            return null;
        }
        
        // 알려진 브랜드와 비교
        for (String brand : KNOWN_BRANDS) {
            if (brandName.contains(brand) || brand.contains(brandName)) {
                return brand;
            }
        }
        
        return brandName;
    }
    
    /**
     * 문자열 길이를 안전하게 읽습니다.
     */
    private int readStringLength(ByteBuffer buffer) {
        if (!buffer.hasRemaining()) {
            return 0;
        }
        int len = buffer.getInt();
        return (len >= 0) ? len : 0; // 음수 길이는 0으로 처리
    }
    
    /**
     * 버퍼에 지정된 바이트 수만큼 더 데이터가 있는지 확인합니다.
     */
    private boolean hasMoreData(ByteBuffer buffer, int bytesNeeded) {
        return buffer.remaining() >= bytesNeeded;
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
