package ru.yandex.practicum.model;

import jakarta.persistence.*;
import lombok.*;
import lombok.experimental.FieldDefaults;

@Entity
@Data
@Table(name = "actions")
@FieldDefaults(level = AccessLevel.PRIVATE)
@SecondaryTable(name = "scenario_actions", pkJoinColumns = @PrimaryKeyJoinColumn(name = "action_id"))
public class Action {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    int id;

    ActionType type;
    int value;

    @ManyToOne
    @JoinColumn(name = "scenario_id", table = "scenario_actions")
    Scenario scenario;

    @ManyToOne()
    @JoinColumn(name = "sensor_id", table = "scenario_actions")
    Sensor sensor;
}