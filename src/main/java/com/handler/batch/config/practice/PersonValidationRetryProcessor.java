package com.handler.batch.config.practice;

import com.handler.batch.dao.Person;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.retry.support.RetryTemplateBuilder;

@Slf4j
public class PersonValidationRetryProcessor implements ItemProcessor<Person, Person> {

    private final RetryTemplate retryTemplate;

    public PersonValidationRetryProcessor() {
        this.retryTemplate = new RetryTemplateBuilder()
                .maxAttempts(3)
                .retryOn(NotFoundNameException.class)
                .withListener(new SavePersonRetryListener())
                .build();
    }

    @Override
    public Person process(Person person) throws Exception {
        return this.retryTemplate.execute(context -> {
            // Retry Callback (maxAttempts count)
            if (person.isNotEmptyName()) return person;

            throw new NotFoundNameException();

        }, context -> {
            // Recovery Callback
            return person.unknownName();
        });
    }

    public static class SavePersonRetryListener implements RetryListener {
        @Override
        public <T, E extends Throwable> boolean open(RetryContext retryContext, RetryCallback<T, E> retryCallback) {
            return true;
        }

        @Override
        public <T, E extends Throwable> void close(RetryContext retryContext, RetryCallback<T, E> retryCallback, Throwable throwable) {
            log.info("close");
        }

        @Override
        public <T, E extends Throwable> void onError(RetryContext retryContext, RetryCallback<T, E> retryCallback, Throwable throwable) {
            log.info("onError");
        }
    }
}
