create table if not exists stg_sales (
    order_id int not null,
    customer_id int not null,
    product_id int not null,
    quantity int not null,
    price float not null,
    order_date date not null,
    store_city varchar(256),
    store_country varchar(16)
);

create table if not exists stg_users (
    id int not null,
    name varchar(256),
    username varchar(256) not null,
    email varchar(256) not null,
    address JSONB,
    phone varchar(256),
    website varchar(256),
    company JSONB
);

create table if not exists stg_weather (
    created_at timestamp not null,
    country varchar(256) not null,
    city varchar(256) not null,
    weather JSONB,
    main JSONB,
    wind JSONB,
    clouds JSONB,
    sys JSONB
)