create table person (
    id bigint primary key auto_increment,
    name varchar(255),
    age varchar(255),
    address varchar(255)
);

insert into person(name, age, address)
values ('노승철','27','인천');
insert into person(name, age, address)
values ('홍길동','30','서울');
insert into person(name, age, address)
values ('아무개','24','강원');
