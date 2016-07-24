#--
#      Copyright (C) 2016 DataStax Inc.
#
#      This software can be used solely with DataStax Enterprise. Please consult the license at
#      http://www.datastax.com/terms/datastax-dse-driver-license-terms
#++

require 'mkmf'

LIBDIR      = RbConfig::CONFIG['libdir']
INCLUDEDIR  = RbConfig::CONFIG['includedir']

HEADER_DIRS = [INCLUDEDIR, '/usr/include'].freeze

LIB_DIRS = [LIBDIR, '/usr/lib'].freeze

dir_config('gssapi', HEADER_DIRS, LIB_DIRS)

abort 'no gssapi header' unless find_header('gssapi/gssapi.h')

abort 'gssapi library not found' unless find_library('gssapi_krb5', 'gss_release_buffer')

# On Mac at least, there are a lot of deprecated api warnings for gss_* functions. Disable that!
# rubocop:disable Style/GlobalVars
$CFLAGS << ' -Wno-deprecated'

create_makefile('gss_api_context/gss_api_context')
