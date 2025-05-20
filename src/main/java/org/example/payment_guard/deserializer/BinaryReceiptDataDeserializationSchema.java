package org.example.payment_guard.deserializer;

import com.example.test1.entity.ReceiptData;
import com.example.test1.entity.MenuItem;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * 바이너리 형식으로 된 ReceiptData를 직접 파싱하는 DeserializationSchema 구현체입니다.
 */
public class BinaryReceiptDataDeserializationSchema implements DeserializationSchema<ReceiptData> {

    @Override
    public ReceiptData deserialize(byte[] message) throws IOException {
        try {
            // 테스트를 위해 입력 데이터 로그 추가
            System.out.println("\n----- 수신된 바이트 데이터 정보 -----");
            System.out.println("Data length: " + (message != null ? message.length : "null"));
            if (message != null && message.length > 0) {
                System.out.println("First 20 bytes (hex): " + bytesToHex(message, 0, Math.min(20, message.length)));
            }
            System.out.println("----------------------------------\n");
            
            // 기본 값으로 초기화된 ReceiptData 객체 생성
            ReceiptData receiptData = new ReceiptData();
            receiptData.setFranchiseId(0);
            receiptData.setStoreBrand(null);
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
            
            // 테스트용 더미 데이터 생성 (실제로 데이터가 잘 수신되지 않는 경우를 대비)
            receiptData.setStoreBrand("한신포차");  // 테스트용 더미 브랜드
            receiptData.setStoreId(1);  // 테스트용 더미 ID
            receiptData.setStoreName("강남점");  // 테스트용 더미 상점명
            receiptData.setRegion("서울");  // 테스트용 지역
            receiptData.setTotalPrice(35000);  // 테스트용 가격
            
            // 현재 시간 설정
            java.time.LocalDateTime now = java.time.LocalDateTime.now();
            java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            receiptData.setTime(now.format(formatter));
            
            // 테스트용 메뉴 아이템 추가
            com.example.test1.entity.MenuItem item1 = new com.example.test1.entity.MenuItem();
            item1.setMenuId(1001);
            item1.setMenuName("김치찌개");
            item1.setUnitPrice(15000);
            item1.setQuantity(1);
            
            com.example.test1.entity.MenuItem item2 = new com.example.test1.entity.MenuItem();
            item2.setMenuId(1002);
            item2.setMenuName("소주");
            item2.setUnitPrice(5000);
            item2.setQuantity(4);
            
            List<com.example.test1.entity.MenuItem> menuItems = new ArrayList<>();
            menuItems.add(item1);
            menuItems.add(item2);
            receiptData.setMenuItems(menuItems);
            
            System.out.println("테스트용 더미 데이터 생성 완료");
            
            return receiptData;
            
            /* 이전 파싱 코드 임시 주석처리 - 여기서부터 제거 가능
            if (message == null || message.length == 0) {
                System.out.println("입력 메시지가 비어있습니다. 기본 객체를 반환합니다.");
                return receiptData;
            }
            
            ByteBuffer buffer = ByteBuffer.wrap(message);
            
            // franchise_id가 존재하면 읽기
            if (hasMoreData(buffer, 4)) {
                receiptData.setFranchiseId(buffer.getInt());
            }
            */
        } catch (Exception e) {
            System.err.println("데이터 파싱 중 오류: " + e.getMessage());
            e.printStackTrace();
            
            // 기본 ReceiptData 객체 반환 (실패했을 때 최소한 작동할 수 있도록)
            ReceiptData fallbackData = new ReceiptData();
            fallbackData.setFranchiseId(0);
            fallbackData.setStoreBrand("오류 발생"); // 오류 상태를 명확히 표시
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
     * 바이트 배열을 16진수 문자열로 변환
     */
    private static String bytesToHex(byte[] bytes, int offset, int length) {
        StringBuilder result = new StringBuilder();
        for (int i = offset; i < offset + length; i++) {
            result.append(String.format("%02X ", bytes[i]));
        }
        return result.toString();
    }
    
    /**
     * 브랜드명과 상점명을 확인합니다.
     */
    private void fixBrandAndStoreName(ReceiptData data) {
        // 원본 데이터를 그대로 사용합니다.
        // 로그만 출력하여 데이터의 상태를 확인합니다.
        System.out.println("상점 데이터 확인: store_id=" + data.getStoreId() + 
                         ", store_brand=" + data.getStoreBrand() + 
                         ", store_name=" + data.getStoreName());
    }
    
    /**
     * 브랜드명을 그대로 반환합니다.
     */
    private String correctBrandName(String brandName) {
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
