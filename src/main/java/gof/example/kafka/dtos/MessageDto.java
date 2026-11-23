package gof.example.kafka.dtos;

import lombok.*;

import java.time.LocalDateTime;


//@AllArgsConstructor
//@NoArgsConstructor
//@Data
//@Builder
//@ToString
//@Getter
//public class MessageDto {
//    private String name;
//    private String message;
//    @Builder.Default
//    private LocalDateTime createdAt = LocalDateTime.now();
//}

public record MessageDto(String name, String message, LocalDateTime createdAt) {
    public MessageDto {
        if (createdAt == null) {
            createdAt = LocalDateTime.now();
        }
    }
}