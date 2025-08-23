package ac.h2.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jdbc.core.JdbcAggregateTemplate;
import org.springframework.data.jdbc.core.convert.*;
import org.springframework.data.jdbc.core.dialect.JdbcOracleDialect;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.OracleDialect;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;

@Configuration
@EnableJdbcRepositories(basePackages = "ac.h2")
public class DatabaseJdbcConfig {

    private final DataSource dataSource;

    public DatabaseJdbcConfig(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Bean
    public JdbcTemplate jdbcTemplate() {
        return new JdbcTemplate(dataSource);
    }

    @Bean
    public NamedParameterJdbcTemplate namedParameterJdbcTemplate() {
        return new NamedParameterJdbcTemplate(dataSource);
    }

    @Bean
    @ConditionalOnProperty(name = "app.database.dialect", havingValue = "oracle", matchIfMissing = true)
    public Dialect dialect() {
        return new JdbcOracleDialect();
    }

//    @Bean
//    public NamingStrategy namingStrategy(@Value("${app.database.schema:}") String schema) {
//        if (StringUtils.hasText(schema)) {
//            return new SchemaPrefixedNamingStrategy(schema.trim());
//        }
//        return NamingStrategy.INSTANCE;
//    }

    @Bean
    public JdbcMappingContext jdbcMappingContext(NamingStrategy namingStrategy) {
        JdbcMappingContext mappingContext = new JdbcMappingContext(namingStrategy);
        try {
            mappingContext.afterPropertiesSet();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize JdbcMappingContext", e);
        }
        return mappingContext;
    }


    @Bean
    public DataAccessStrategy dataAccessStrategy(
            NamedParameterJdbcTemplate namedParameterJdbcTemplate,
            JdbcConverter jdbcConverter,
            JdbcMappingContext mappingContext,
            Dialect dialect) {

        SqlGeneratorSource sqlGeneratorSource = new SqlGeneratorSource(
                mappingContext,
                jdbcConverter,
                dialect
        );

        return new DefaultDataAccessStrategy(
                sqlGeneratorSource,
                mappingContext,
                jdbcConverter,
                namedParameterJdbcTemplate,
                new SqlParametersFactory( mappingContext, jdbcConverter),
                new InsertStrategyFactory(namedParameterJdbcTemplate, dialect)
        );

    }

}