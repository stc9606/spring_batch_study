package com.handler.batch.config.practice2;


import com.handler.batch.config.practice3.Orders;
import com.mysql.cj.xdevapi.Collection;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SaveUserTasklet implements Tasklet {

    private final int size = 10000;

    private final UserRepository userRepository;

    public SaveUserTasklet(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        List<User> users = createUsers();

        Collections.shuffle(users);

        userRepository.saveAll(users);

        return RepeatStatus.FINISHED;
    }

    private List<User> createUsers() {
        List<User> users = new ArrayList<>();

        for (int i=0; i<size; i++) {
            users.add(User.builder()
                    .orders(Collections.singletonList(Orders.builder()
                            .amount(1_000)
                            .createdDate(LocalDate.of(2022, 7, 1))
                            .itemName("item + "+ i)
                            .build()))
                    .username("test username"+i)
                    .build()
            );
        }

        for (int i = 0; i < size; i++) {
            users.add(User.builder()
                    .orders(Collections.singletonList(Orders.builder()
                            .amount(200_000)
                            .createdDate(LocalDate.of(2022, 7, 2))
                            .itemName("item + "+ i)
                            .build()))
                    .username("test username"+i)
                    .build()
            );
        }

        for (int i = 0; i < size; i++) {
            users.add(User.builder()
                    .orders(Collections.singletonList(Orders.builder()
                            .amount(300_000)
                            .createdDate(LocalDate.of(2022, 7, 3))
                            .itemName("item + "+ i)
                            .build()))
                    .username("test username"+i)
                    .build()
            );
        }

        for (int i = 0; i < size; i++) {
            users.add(User.builder()
                    .orders(Collections.singletonList(Orders.builder()
                            .amount(500_000)
                            .createdDate(LocalDate.of(2022, 7, 4))
                            .itemName("item + "+ i)
                            .build()))
                    .username("test username"+i)
                    .build()
            );
        }

        return users;
    }
}
