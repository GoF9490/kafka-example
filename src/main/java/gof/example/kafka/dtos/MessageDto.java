package gof.example.kafka.dtos;

import lombok.*;

import java.time.LocalDateTime;


@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@ToString
@Getter
public class MessageDto {
    private String name;
    private String message;
    @Builder.Default
    private LocalDateTime createdAt = LocalDateTime.now();
}