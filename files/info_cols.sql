/*
select distinct b.TABLE_SCHEMA
              , b.TABLE_NAME
              , b.COLUMN_NAME
              , b.ORDINAL_POSITION
              , b.COLUMN_TYPE
              , b.COLUMN_KEY
              , b.COLUMN_DEFAULT
from LNK.TABLES a
         inner join LNK.COLUMNS b
                    on (a.TABLE_SCHEMA = b.TABLE_SCHEMA and a.TABLE_NAME = b.TABLE_NAME)
where 1 = 1
  and a.TABLE_SCHEMA IN ('DB1', 'DB2', 'DB3')
  and (b.TABLE_SCHEMA, b.TABLE_NAME, b.COLUMN_NAME, b.ORDINAL_POSITION, b.COLUMN_TYPE, b.COLUMN_KEY, b.COLUMN_DEFAULT) not in
      (select bb.TABLE_SCHEMA
            , bb.TABLE_NAME
            , bb.COLUMN_NAME
            , bb.ORDINAL_POSITION
            , bb.COLUMN_TYPE
            , bb.COLUMN_KEY
            , bb.COLUMN_DEFAULT
       from information_schema.TABLES aa
                inner join information_schema.COLUMNS bb
                           on (aa.TABLE_SCHEMA = bb.TABLE_SCHEMA and aa.TABLE_NAME = bb.TABLE_NAME)
       where 1 = 1
         and aa.TABLE_SCHEMA = a.TABLE_SCHEMA
         and aa.TABLE_NAME = a.TABLE_NAME)
;
*/

select distinct b.TABLE_SCHEMA
              , b.TABLE_NAME
              , b.COLUMN_NAME
              , b.ORDINAL_POSITION
              , b.COLUMN_TYPE
              , b.COLUMN_KEY
              , b.COLUMN_DEFAULT
from LNK.TABLES a
         inner join LNK.COLUMNS b
                    on (a.TABLE_SCHEMA = b.TABLE_SCHEMA and a.TABLE_NAME = b.TABLE_NAME)
where 1 = 1
  and a.TABLE_SCHEMA IN ('DB1', 'DB2', 'DB3')
  and not exists(
        select 1
        from information_schema.TABLES aa
                 inner join information_schema.COLUMNS bb
                            on (aa.TABLE_SCHEMA = bb.TABLE_SCHEMA and aa.TABLE_NAME = bb.TABLE_NAME)
        where 1 = 1
          and aa.TABLE_SCHEMA = a.TABLE_SCHEMA
          and aa.TABLE_NAME = a.TABLE_NAME
          and bb.COLUMN_NAME = b.COLUMN_NAME
          and bb.ORDINAL_POSITION = b.ORDINAL_POSITION
          and bb.COLUMN_TYPE = b.COLUMN_TYPE
          and bb.COLUMN_KEY = b.COLUMN_KEY
          and IFNULL(bb.COLUMN_DEFAULT, '') = IFNULL(b.COLUMN_DEFAULT, '')
    )
  and (a.TABLE_SCHEMA, a.TABLE_NAME) NOT IN ({0})
;