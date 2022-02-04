include env.include

.SUFFIXES:

VPATH := ./$(SRC)_export



# ##############################
# Check inputs
#
$(call assert,$(SRC),Must set SRC)
$(call assert,$(ENV),Must set ENV)

# Get list of import files
EXPORTS := $(wildcard $(QDC_SOURCE)_export/*zip)
$(call info-msg, $(words $(EXPORTS)) files to import)

$(foreach exp,$(EXPORTS),$(call info-msg, $(exp)))

# ##############################
# Defaults
#
TS := $(strip $(shell date '+%Y%m%d'))
YML := pd_aws_$(call get,envs,$(ENV)).yml

.PHONY: all
all: IMPORT

.PHONY: IMPORT
IMPORT: $(EXPORTS)

.PHONY: $(EXPORTS)
.ONESHELL:
$(EXPORTS):
	cd ..
	./pd_shell.sh -y $(YML) -i -s $(QDC_SOURCE) -e ./swat/$@
