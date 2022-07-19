package com.handler.batch.config.practice2;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.time.LocalDate;
import java.util.Collection;

public interface UserRepository extends JpaRepository<User, Long> {
    Collection<User> findAllByUpdatedDate(LocalDate now);

    @Query(value = "select min(u.id) from User u")
    long findMinId();

    @Query(value = "select max(u.id) from User u")
    long findMaxId();
}
