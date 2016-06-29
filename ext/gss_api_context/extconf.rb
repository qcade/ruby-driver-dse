#--
#      Copyright (C) 2016 DataStax Inc.
#
#      This software can be used solely with DataStax Enterprise. Please consult the license at
#      http://www.datastax.com/terms/datastax-dse-driver-license-terms
#++

require 'mkmf'

LIBDIR      = RbConfig::CONFIG['libdir']
INCLUDEDIR  = RbConfig::CONFIG['includedir']

HEADER_DIRS = [INCLUDEDIR, '/usr/include']

LIB_DIRS = [LIBDIR, '/usr/lib']

dir_config('gssapi', HEADER_DIRS, LIB_DIRS)

unless find_header('gssapi/gssapi.h')
  abort "no gssapi header"
end

unless find_library('gssapi_krb5', 'gss_release_buffer')
  abort "gssapi library not found"
end

# On Mac at least, there are a lot of deprecated api warnings for gss_* functions. Disable that!
$CFLAGS << " -Wno-deprecated"

create_makefile('gss_api_context/gss_api_context')
