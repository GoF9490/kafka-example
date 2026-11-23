package gof.example.kafka.dtos;

public record CategoryStats(long count, long totalAmount, double averageAmount) {
    public CategoryStats add(long amount) {
        long newCount = this.count + 1;
        long newTotal = this.totalAmount + amount;
        return new CategoryStats(newCount, newTotal, (double) newTotal / newCount);
    }
}
