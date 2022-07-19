package com.handler.batch.config.practice4;

import com.handler.batch.config.practice2.LevelUpJobExecutionListener;
import com.handler.batch.config.practice2.SaveUserTasklet;
import com.handler.batch.config.practice2.User;
import com.handler.batch.config.practice2.UserRepository;
import com.handler.batch.config.practice3.JobParametersDecide;
import com.handler.batch.config.practice3.OrderStatistics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

@Configuration
@Slf4j
@RequiredArgsConstructor
public class ParallelUserConfiguration {

    private final String JOB_NAME = "parallelUserJob";
    private final int CHUNK = 1000;

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final UserRepository userRepository;
    private final EntityManagerFactory entityManagerFactory;
    private final DataSource dataSource;
    private final TaskExecutor taskExecutor;


    @Bean(JOB_NAME)
    public Job userJob() throws Exception {
        return this.jobBuilderFactory.get(JOB_NAME)
                .incrementer(new RunIdIncrementer())
                .listener(new LevelUpJobExecutionListener(userRepository))
                .start(this.saveUserFlow())
                .next(this.splitFlow(null))
                .build()
                .build();
    }

    @Bean(JOB_NAME+"_saveUserFLow")
    public Flow saveUserFlow() {
        TaskletStep saveUserStep = this.stepBuilderFactory.get(JOB_NAME + "_saveUserStep")
                .tasklet(new SaveUserTasklet(userRepository))
                .build();

        return new FlowBuilder<SimpleFlow>(JOB_NAME+"_saveUserFLow")
                .start(saveUserStep)
                .build();
    }

    @Bean(JOB_NAME+"_saveUserStep")
    public Step saveUserStep() {
        return this.stepBuilderFactory.get(JOB_NAME+"_saveUserStep")
                .tasklet(new SaveUserTasklet(userRepository))
                .build();
    }

    @Bean(JOB_NAME+"+userLevelUpStep")
    public Step userLevelUpStep() throws Exception {
        return stepBuilderFactory.get(JOB_NAME+"+userLevelUpStep")
                .<User, User>chunk(CHUNK)
                .reader(itemReader(null, null))
                .processor(itemProcessor())
                .writer(itemWriter())
                .build();
    }

    @Bean(JOB_NAME+"_userLevelUpStep.manager")
    public Step userLevelUpManagerStep() throws Exception {
        return this.stepBuilderFactory.get(JOB_NAME+"_userLevelUpStep.manager")
                .partitioner(JOB_NAME+"_userLevelUpStep", new UserLevelUpPartitioner(userRepository))
                .step(userLevelUpStep())
                .partitionHandler(taskExecutorPartitionHandler())
                .build();
    }

    @Bean(JOB_NAME+"_taskExecutorPartitionHandler")
    PartitionHandler taskExecutorPartitionHandler() throws Exception {
        TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();

        handler.setStep(userLevelUpStep());
        handler.setTaskExecutor(this.taskExecutor);
        handler.setGridSize(8);

        return handler;
    }

    @Bean(JOB_NAME+"_splitFlow")
    @JobScope
    public Flow splitFlow(@Value("#{jobParameters[date]}") String date) throws Exception {
        Flow userLevelUpFlow = new FlowBuilder<SimpleFlow>(JOB_NAME+"_userLevelUpFlow")
                .start(userLevelUpManagerStep())
                .build();

        // 각 Step을 Flow로 감싼 이유는 2개의 Step의 Flow를 1개의 Flow로 감싸기 위해서.
        return new FlowBuilder<SimpleFlow>(JOB_NAME+"_splitFlow")
                .split(this.taskExecutor)
                .add(userLevelUpFlow, orderStatisticsFlow(date)) // step 병렬로 처리
                .build();
    }


    private Flow orderStatisticsFlow(String date) throws Exception {
        return new FlowBuilder<SimpleFlow>(JOB_NAME+"_orderStatisticsFlow")
                .start(new JobParametersDecide("date"))
                .on(JobParametersDecide.CONTINUE.getName())
                .to(this.orderStatisticsStep(date))
                .build();
    }

    private Step orderStatisticsStep(@Value("#{jobParameters[date]}") String date) throws Exception {
        return this.stepBuilderFactory.get(JOB_NAME+"_orderStatisticsStep")
                .<OrderStatistics, OrderStatistics>chunk(CHUNK)
                .reader(orderStatisticsItemReader(date))
                .writer(orderStatisticsItemWriter(date))
                .build();
    }

    private ItemWriter<? super OrderStatistics> orderStatisticsItemWriter(String date) throws Exception {
        YearMonth yearMonth = YearMonth.parse(date);

        String fileName = yearMonth.getYear() + "년_"+yearMonth.getMonthValue() + "월_일별_주문_금액.csv";

        BeanWrapperFieldExtractor<OrderStatistics> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[] {"amount", "date"});

        DelimitedLineAggregator<OrderStatistics> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");
        lineAggregator.setFieldExtractor(fieldExtractor);

        FlatFileItemWriter<OrderStatistics> itemWriter = new FlatFileItemWriterBuilder<OrderStatistics>()
                .resource(new FileSystemResource("output/" + fileName))
                .lineAggregator(lineAggregator)
                .name(JOB_NAME+"_orderStatisticsItemWriter")
                .encoding("UTF-8")
                .headerCallback(writer -> writer.write("total_amount,date"))
                .build();

        // 파일의 경우 chunk의 step이 완전히 끝났을 때 메모리에 저장해 놓은 데이터를 가지고 파일을 한 번에 생성.

        itemWriter.afterPropertiesSet();
        return itemWriter;
    }

    private ItemReader<? extends OrderStatistics> orderStatisticsItemReader(String date) throws Exception {
        YearMonth yearMonth = YearMonth.parse(date);

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("startDate", yearMonth.atDay(1));
        parameters.put("endDate", yearMonth.atEndOfMonth());

        Map<String, Order> sortKey = new HashMap<>();
        sortKey.put("created_date", Order.ASCENDING);

        JdbcPagingItemReader<OrderStatistics> itemReader = new JdbcPagingItemReaderBuilder<OrderStatistics>()
                .dataSource(this.dataSource)
                .rowMapper((resultSet, i) -> OrderStatistics.builder()
                        .amount(resultSet.getString(1))
                        .date(LocalDate.parse(resultSet.getString(2), DateTimeFormatter.ISO_DATE))
                        .build())
                .pageSize(CHUNK)
                .name(JOB_NAME+"_orderStatisticsItemReader")
                .selectClause("sum(amount), created_date")
                .fromClause("orders")
                .whereClause("created_date > :startDate and created_date <= :endDate")
                .groupClause("created_date")
                .parameterValues(parameters)
                .sortKeys(sortKey)
                .build();

        itemReader.afterPropertiesSet();

        return itemReader;
    }


    private ItemWriter<? super User> itemWriter() {
        return users -> {
            users.forEach(x -> {
                x.levelUp();
                userRepository.save(x);
            });
        };
    }

    private ItemProcessor<? super User,? extends User> itemProcessor() {
        return user -> {
            if (user.availableLevelUp()) { // 등급 상향 대상 체크
                return user;
            }

            return null;
        };
    }

    @Bean(JOB_NAME+"_userItemReader")
    @StepScope
    JpaPagingItemReader<? extends User> itemReader(@Value("#{stepExecutionContext[minId]}") Long minId,
                                          @Value("#{stepExecutionContext[maxId]}") Long maxId) throws Exception {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("minId", minId);
        parameters.put("maxId", maxId);

        JpaPagingItemReader<User> itemReader = new JpaPagingItemReaderBuilder<User>()
                .queryString("select u from User u where u.id between :minId and :maxId")
                .parameterValues(parameters)
                .entityManagerFactory(entityManagerFactory)
                .pageSize(CHUNK)
                .name(JOB_NAME+"_userItemReader")
                .build();

        itemReader.afterPropertiesSet();
        return itemReader;
    }
}
