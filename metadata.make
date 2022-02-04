include env.include

.SUFFIXES:

# #####################################
# Directory paths to search for
# dependencies
#

vpath %.cols ./$(ENV)/$(SRC)/$(META)
vpath %.count ./$(ENV)/$(SRC)/$(META)

# #####################################
# Macros
#
define get-src-count

	$(call info-msg, Performing count on $(SRC_DB) $(basename $@))
	@echo "SET NOCOUNT ON" > $(SRC)_counts.sql
	@echo "SELECT CONVERT(VARCHAR(20), getdate(), 121) + ',' +" >> $(SRC)_counts.sql
	@echo "'$(ENV)' + ',' + '$(SRC)' + ',' + '$(basename $@)' + ',' +" >> $(SRC)_counts.sql
	@echo "CAST(COUNT(*) AS VARCHAR) FROM $(basename $@);" >> $(SRC)_counts.sql
	@echo "GO" >> $(SRC)_counts.sql

	@$(SQLCMD) $(SQLCMD_OPT) \
		$(SRC_CONN) \
		-i $(SRC)_counts.sql >> ./$(ENV)/$(SRC)/$(META)/$(basename $@).count

endef

define get-src-metadata

	$(call info-msg, Extracting metadata for $(SRC_DB) $(basename $@))
	@$(SQLCMD) $(SQLCMD_OPT) \
		$(SRC_CONN) \
		-v TableName="$(basename $@)" \
		-i get_table_metadata.sql \
		-o ./$(ENV)/$(SRC)/$(META)/$@ || echo "**** $(SRC_DB), $@ - $(ENV), metadata gathering failed"

endef

# #####################################
# Source connections
#
ifeq ($(SRC_AON),N)
SRC_CONN     = -S $(SRC_HOST)\\$(SRC_INST) -d $(SRC_DB) -U $(SRC_USR) -P '$(SRC_PWD)'
else
SRC_CONN     = -S "$(SRC_HOST),$(SRC_PORT)" -d $(SRC_DB) -U $(SRC_USR) -P '$(SRC_PWD)'
endif

# #####################################
# Tables to be referenced
#
COLS = $(patsubst %,%.cols,$(SRC_TABLES))

# #####################################
# Rules and recipies
#
.PHONY: all
all: ./$(ENV)/$(SRC)/$(META) get_table_metadata $(COLS)

./$(ENV)/$(SRC)/$(META): 
	@[ -d $@ ] || mkdir -p $@
	@[ -d $@ ] && echo "**** Directory $@ exists"


# ######################################
# Build sql cmd file if not exists
#
get_table_metadata: get_table_metadata.sql

get_table_metadata.sql:
	echo "SET NOCOUNT ON;" > $@
	echo "SELECT CAST(ORDINAL_POSITION AS VARCHAR) + ',' + " >> $@
	echo "    COLUMN_NAME + ',' + " >> $@
	echo "    DATA_TYPE + ',' +" >> $@
	echo "    CASE DATA_TYPE " >> $@
	echo "      WHEN 'VARCHAR' THEN CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR)" >> $@
	echo "      WHEN 'CHAR' THEN CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR)" >> $@
	echo "      ELSE COALESCE(CAST(NUMERIC_PRECISION AS VARCHAR),'')" >> $@
	echo "    END + ',' +" >> $@
	echo "    COALESCE(CAST(NUMERIC_SCALE AS VARCHAR),'') + ',' +" >> $@
	echo "    CAST(IS_NULLABLE AS VARCHAR) + ',' +" >> $@
	echo "    CAST(COLUMNPROPERTY(object_id(TABLE_SCHEMA+'.'+TABLE_NAME), COLUMN_NAME, 'IsIdentity') AS VARCHAR)" >> $@
	echo "FROM INFORMATION_SCHEMA.COLUMNS" >> $@
	echo "WHERE TABLE_NAME='\$(DOLLAR)(TableName)' AND" >> $@
	echo "TABLE_SCHEMA = 'dbo';" >> $@
	echo "GO" >> $@

# ##################################################
# Get the source table counts
#

$(COLS):
	-$(get-src-count)
	-$(get-src-metadata)


# ##################################################
# Info
#
.PHONY: info
info:
	@echo
	$(call info-msg,                 Target DB: $(TGT_DB))
	$(call info-msg,               Target Host: $(TGT_HOST))
	$(call info-msg,           Target Instance: $(TGT_INST))
	$(call info-msg,               Target Port: $(TGT_PORT))
	$(call info-msg,               Target User: $(TGT_USR))
	$(call info-msg,                Target Pwd: $(TGT_PWD))
	$(call info-msg,             Source Tables)
	$(call info-msg,             =============)
	@$(foreach s,$(sort $(SRC_TABLES)),$(call info-msg,              $s))
	$(call info-msg,             Target Tables)
	$(call info-msg,             =============)
	@$(foreach s,$(sort $(TGT_TABLES)),$(call info-msg,              $s))

# ##################################################
# Clean
#
.PHONY: clean
clean: clean_cols

.PHONY: clean_cols
clean_cols:
	$(RM) $(RM_OPT) ./$(ENV)/$(SRC)/$(META)/*cols

.PHONY: clean_counts
clean_counts:
	$(RM)  $(RM_OPT)./$(ENV)/$(SRC)/$(META)/$(call lc,$(SRC))_*count
