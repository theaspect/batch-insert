Run docker image:

```bash
docker run \
    -p 3306:3306 \
    --name=mysql \
    -e MYSQL_ROOT_PASSWORD=pass \
    -e MYSQL_ROOT_HOST=% \
    -e MYSQL_DATABASE=db \
    -d mysql/mysql-server
```

Test connection:
```bash
mysql -h localhost -u root -P 3306 -ppass --protocol=tcp
```

xml template
```xml
<?xml version="1.0" encoding="UTF-8"?>
<voters>
    <voter name="Иван Иванов" birthDay="1943.22.19">
        <visit station="121" time="2016.10.18 16:52:41" />
    </voter>
    ...
</voters>
```

Put files into `src/main/resources`
