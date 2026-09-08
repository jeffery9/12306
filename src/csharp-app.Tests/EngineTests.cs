using Xunit;
using TicketingApp;

namespace TicketingApp.Tests;

public class EngineTests
{
    [Fact]
    public void TestGetMask()
    {
        // 验证原子段位图区间预占掩码计算
        Assert.Equal(1, TicketingEngine.GetMask(1, 2)); // 区间1: 2^0 = 1
        Assert.Equal(2, TicketingEngine.GetMask(2, 3)); // 区间2: 2^1 = 2
        Assert.Equal(3, TicketingEngine.GetMask(1, 3)); // 跨区间[1-3]: 2^0 + 2^1 = 3
        Assert.Equal(4, TicketingEngine.GetMask(3, 4)); // 区间3: 2^2 = 4
        Assert.Equal(6, TicketingEngine.GetMask(2, 4)); // 跨区间[2-4]: 2^1 + 2^2 = 6
        Assert.Equal(7, TicketingEngine.GetMask(1, 4)); // 全区间[1-4]: 2^0 + 2^1 + 2^2 = 7
    }

    [Fact]
    public void TestGuidFormat()
    {
        // 验证 C# 唯一主键生成规范与 36 字节 Hyphens 分割
        string uuid = Guid.NewGuid().ToString();
        Assert.Equal(36, uuid.Length);
        
        int hyphens = uuid.Length - uuid.Replace("-", "").Length;
        Assert.Equal(4, hyphens);
    }
}
