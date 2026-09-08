package com.ticket;

import org.junit.jupiter.api.Test;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class TicketingApplicationTests {

    @Test
    void testGetMask() {
        TicketingService service = new TicketingService(null, null, null, null);
        
        // 验证原子段位图区间预占掩码计算
        assertEquals(1, service.getMask(1, 2)); // 区间1: 2^0 = 1
        assertEquals(2, service.getMask(2, 3)); // 区间2: 2^1 = 2
        assertEquals(3, service.getMask(1, 3)); // 跨区间[1-3]: 2^0 + 2^1 = 3
        assertEquals(4, service.getMask(3, 4)); // 区间3: 2^2 = 4
        assertEquals(6, service.getMask(2, 4)); // 跨区间[2-4]: 2^1 + 2^2 = 6
        assertEquals(7, service.getMask(1, 4)); // 全区间[1-4]: 2^0 + 2^1 + 2^2 = 7
    }

    @Test
    void testGuidFormat() {
        // 验证 Java 唯一主键生成规范与 36 字节 Hyphens 分割
        String uuid = UUID.randomUUID().toString();
        assertEquals(36, uuid.length());
        
        int hyphens = uuid.length() - uuid.replace("-", "").length();
        assertEquals(4, hyphens);
    }
}
