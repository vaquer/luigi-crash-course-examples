# EJEMPLO PIPELINE QQP

### Creacion de tablas
```sql
CREATE TABLE PRODUCTO (
    id char(30) CONSTRAINT firstkey PRIMARY KEY,
    product varchar(100),
    presentacion varchar(100),
    marca char(20),
    categoria varchar(100),
    catalogo varchar(100),
    precio decimal,
    fechaRegistro date,
    cadenaComercial varchar(100),
    giro varchar(100),
    rfc varchar(100),
    razonSocial varchar(100),
    nombreComercial varchar(100),
    direccion varchar(500),
    estado varchar(100),
    municipio varchar(100),
    latitud float,
    longitud float
);
```