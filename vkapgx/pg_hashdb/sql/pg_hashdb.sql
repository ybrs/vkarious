-- These functions use SPI and scan tables; they must not be immutable or parallel safe.
create function vkar_hash_table(regclass, int)
  returns text
  strict
  volatile
  parallel unsafe;

create function vkar_db_hash(int)
  returns table(rel text, digest text)
  volatile
  parallel unsafe;
