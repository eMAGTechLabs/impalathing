
thrift:
	thrift -r -gen go:package_prefix=github.com/eMAGTechLabs/impalathing/services/ interfaces/ImpalaService.thrift
	rm -rf ./services
	mv gen-go services
