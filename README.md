@Configuration
@EnableJdbcRepositories
public class JdbcOracleConfiguration extends AbstractJdbcConfiguration {

    @Bean
    public DataSource dataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:oracle:thin:@localhost:1521:XE");
        config.setUsername("your_username");
        config.setPassword("your_password");
        config.setDriverClassName("oracle.jdbc.OracleDriver");
        
        // Oracle-specific optimizations
        config.setMaximumPoolSize(20);
        config.setMinimumIdle(5);
        config.setConnectionTimeout(30000);
        config.setIdleTimeout(600000);
        config.setMaxLifetime(1800000);
        
        // Oracle connection properties
        config.addDataSourceProperty("oracle.net.CONNECT_TIMEOUT", "30000");
        config.addDataSourceProperty("oracle.net.READ_TIMEOUT", "60000");
        config.addDataSourceProperty("oracle.jdbc.ReadTimeout", "60000");
        
        return new HikariDataSource(config);
    }

    @Bean
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean
    public NamedParameterJdbcTemplate namedParameterJdbcTemplate(DataSource dataSource) {
        return new NamedParameterJdbcTemplate(dataSource);
    }

    @Bean
    public JdbcAggregateTemplate jdbcAggregateTemplate(
            ApplicationContext applicationContext,
            JdbcMappingContext mappingContext,
            JdbcConverter converter,
            DataAccessStrategy dataAccessStrategy) {
        
        return new JdbcAggregateTemplate(
            applicationContext,
            mappingContext,
            converter,
            dataAccessStrategy
        );
    }

    @Bean
    public DataAccessStrategy dataAccessStrategy(
            NamedParameterJdbcTemplate template,
            JdbcConverter jdbcConverter,
            JdbcMappingContext mappingContext,
            Dialect dialect) {
        
        return new DefaultDataAccessStrategy(
            new SqlGeneratorSource(mappingContext, jdbcConverter, dialect),
            mappingContext,
            jdbcConverter,
            template
        );
    }

    @Bean
    public JdbcMappingContext jdbcMappingContext(Optional<NamingStrategy> namingStrategy) {
        JdbcMappingContext mappingContext = new JdbcMappingContext(namingStrategy.orElse(NamingStrategy.INSTANCE));
        return mappingContext;
    }

    @Bean
    public JdbcConverter jdbcConverter(
            JdbcMappingContext mappingContext,
            NamedParameterJdbcTemplate template,
            @Lazy RelationResolver relationResolver,
            JdbcCustomConversions conversions,
            Dialect dialect) {
        
        DefaultJdbcTypeFactory jdbcTypeFactory = new DefaultJdbcTypeFactory(template.getJdbcTemplate());
        return new MappingJdbcConverter(mappingContext, relationResolver, conversions, jdbcTypeFactory);
    }

    @Bean
    public JdbcCustomConversions jdbcCustomConversions() {
        List<Converter<?, ?>> converters = new ArrayList<>();
        
        // Add Oracle-specific converters if needed
        converters.add(new TimestampToInstantConverter());
        converters.add(new InstantToTimestampConverter());
        
        return new JdbcCustomConversions(converters);
    }

    @Bean
    public Dialect jdbcDialect(NamedParameterJdbcTemplate template) {
        return OracleDialect.INSTANCE;
    }

    // Oracle-specific custom converters
    @WritingConverter
    static class InstantToTimestampConverter implements Converter<Instant, Timestamp> {
        @Override
        public Timestamp convert(Instant source) {
            return source != null ? Timestamp.from(source) : null;
        }
    }

    @ReadingConverter
    static class TimestampToInstantConverter implements Converter<Timestamp, Instant> {
        @Override
        public Instant convert(Timestamp source) {
            return source != null ? source.toInstant() : null;
        }
    }

    // Optional: Custom naming strategy for Oracle conventions
    @Bean
    public NamingStrategy namingStrategy() {
        return new NamingStrategy() {
            @Override
            public String getTableName(Class<?> type) {
                return type.getSimpleName().toUpperCase();
            }

            @Override
            public String getColumnName(RelationalPersistentProperty property) {
                return property.getName().toUpperCase();
            }
        };
    }
}

// Example aggregate entity
@Table("ORDERS")
public class Order {
    @Id
    private Long id;
    
    @Column("CUSTOMER_ID")
    private Long customerId;
    
    @Column("ORDER_DATE")
    private Instant orderDate;
    
    @Column("TOTAL_AMOUNT")
    private BigDecimal totalAmount;
    
    @MappedCollection(idColumn = "ORDER_ID", keyColumn = "LINE_NUMBER")
    private List<OrderLine> orderLines = new ArrayList<>();
    
    // Constructors, getters, setters
    public Order() {}
    
    public Order(Long customerId, Instant orderDate, BigDecimal totalAmount) {
        this.customerId = customerId;
        this.orderDate = orderDate;
        this.totalAmount = totalAmount;
    }
    
    // Getters and setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    
    public Long getCustomerId() { return customerId; }
    public void setCustomerId(Long customerId) { this.customerId = customerId; }
    
    public Instant getOrderDate() { return orderDate; }
    public void setOrderDate(Instant orderDate) { this.orderDate = orderDate; }
    
    public BigDecimal getTotalAmount() { return totalAmount; }
    public void setTotalAmount(BigDecimal totalAmount) { this.totalAmount = totalAmount; }
    
    public List<OrderLine> getOrderLines() { return orderLines; }
    public void setOrderLines(List<OrderLine> orderLines) { this.orderLines = orderLines; }
}

@Table("ORDER_LINES")
class OrderLine {
    @Column("LINE_NUMBER")
    private Integer lineNumber;
    
    @Column("PRODUCT_ID")
    private Long productId;
    
    @Column("QUANTITY")
    private Integer quantity;
    
    @Column("PRICE")
    private BigDecimal price;
    
    // Constructors, getters, setters
    public OrderLine() {}
    
    public OrderLine(Integer lineNumber, Long productId, Integer quantity, BigDecimal price) {
        this.lineNumber = lineNumber;
        this.productId = productId;
        this.quantity = quantity;
        this.price = price;
    }
    
    // Getters and setters
    public Integer getLineNumber() { return lineNumber; }
    public void setLineNumber(Integer lineNumber) { this.lineNumber = lineNumber; }
    
    public Long getProductId() { return productId; }
    public void setProductId(Long productId) { this.productId = productId; }
    
    public Integer getQuantity() { return quantity; }
    public void setQuantity(Integer quantity) { this.quantity = quantity; }
    
    public BigDecimal getPrice() { return price; }
    public void setPrice(BigDecimal price) { this.price = price; }
}
