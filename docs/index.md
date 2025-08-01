
# SedonaDB

SedonaDB is a high-performance, dependency-free geospatial compute engine.

```python
# pip install apache-sedona[db]
import sedona.db

sd = sedona.db.connect()
sd.sql("SELECT ST_Point(0, 1) as geom")
```
