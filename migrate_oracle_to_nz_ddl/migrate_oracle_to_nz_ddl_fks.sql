% if len(table.foreign_keys) > 0:
--------------------
-- ${table.full_table_name}
--------------------
% endif
% for f in table.foreign_keys:
    ALTER TABLE ${table.full_table_name}
        ADD CONSTRAINT ${f.fk_name}
            FOREIGN KEY ( ${f.fkcolumn_name} ) REFERENCES ${settings['table_prefix']}${f.pktable_name}${settings['table_postfix']} ( ${f.pkcolumn_name} )
    ;
% endfor
