// Copyright 2013-2016 DataStax, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <ruby.h>

#include "kerberosgss.h"

//-----------------------------------------------------------------------------
// Platform-specific functions and macros

#ifdef _MSC_VER
typedef unsigned __int64 uint64_t;
typedef __int64 int64_t;

#else
#include <stdint.h>
#endif

VALUE e_GssError;
static void rb_free_context(gss_client_state* state);

static gss_client_state* get_state(VALUE self) {
  gss_client_state* p;
  Data_Get_Struct(self, gss_client_state, p);
  return p;
}

static VALUE
rb_context_alloc(VALUE klass)
{
    gss_client_state* state = ALLOC(gss_client_state);
    state->response = NULL;
    return Data_Wrap_Struct(klass, 0, rb_free_context, state);
}

static VALUE
rb_context_initialize(VALUE self, VALUE _service, VALUE _principal)
{
    const char* service = StringValuePtr(_service);
    const char* principal = _principal == Qnil ? NULL : StringValuePtr(_principal);

    long int gss_flags = GSS_C_MUTUAL_FLAG | GSS_C_SEQUENCE_FLAG;
    gss_client_state* state = get_state(self);

    authenticate_gss_client_init(service, principal, gss_flags, NULL, GSS_C_NO_OID, state);

    return self;
}

static void
rb_free_context(gss_client_state* state)
{
    if (state) {
        authenticate_gss_client_clean(state);
        xfree(state);
    }
}

static VALUE
rb_context_response(VALUE self)
{
    gss_client_state* state = get_state(self);
    return state->response ? rb_str_new(state->response, state->responseLen) : Qnil;
}

static VALUE
rb_context_step(VALUE self, VALUE rb_challenge)
{
    const char* challenge = StringValuePtr(rb_challenge);
    int challenge_len = RSTRING_LEN(rb_challenge);

    gss_client_state* state = get_state(self);
    VALUE rc = INT2NUM(authenticate_gss_client_step(state, challenge, challenge_len));
    return rb_ary_new3(2, rc, rb_context_response(self));
}

static VALUE
rb_context_wrap(VALUE self, VALUE rb_challenge)
{
    const char* challenge = StringValuePtr(rb_challenge);
    int challenge_len = RSTRING_LEN(rb_challenge);

    gss_client_state* state = get_state(self);
    authenticate_gss_client_wrap(state, challenge, challenge_len);
    return rb_context_response(self);
}

static VALUE
rb_context_unwrap(VALUE self, VALUE rb_challenge)
{
    const char* challenge = StringValuePtr(rb_challenge);
    int challenge_len = RSTRING_LEN(rb_challenge);

    gss_client_state* state = get_state(self);
    authenticate_gss_client_unwrap(state, challenge, challenge_len);
    return rb_context_response(self);
}

static VALUE
rb_context_user_name(VALUE self)
{
    gss_client_state* state = get_state(self);
    return state->username ? rb_str_new2(state->username) : Qnil;
}

void
Init_gss_api_context()
{
  VALUE currentContainer;

  currentContainer = rb_define_module_under(rb_cObject, "Dse");
  currentContainer = rb_define_module_under(currentContainer, "Auth");
  currentContainer = rb_define_module_under(currentContainer, "Providers");
  currentContainer = rb_define_class_under(currentContainer, "GssApiContext", rb_cObject);
  rb_define_alloc_func(currentContainer, rb_context_alloc);
  rb_define_method(currentContainer, "initialize", rb_context_initialize, 2);
  rb_define_method(currentContainer, "step", rb_context_step, 1);
  rb_define_method(currentContainer, "response", rb_context_response, 0);
  rb_define_method(currentContainer, "wrap", rb_context_wrap, 1);
  rb_define_method(currentContainer, "unwrap", rb_context_unwrap, 1);
  rb_define_method(currentContainer, "user_name", rb_context_user_name, 0);

  e_GssError = rb_define_class_under(currentContainer, "Error", rb_eStandardError);
}
