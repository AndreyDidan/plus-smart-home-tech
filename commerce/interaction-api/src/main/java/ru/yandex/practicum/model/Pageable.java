package ru.yandex.practicum.model;

import jakarta.validation.constraints.Min;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Pageable {

    @Min(0)
    private Integer page;

    @Min(1)
    private Integer size;

    private List<String> sort;
}