/**
 * Copyright (c) 2006-2016 Apple Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

#include "kerberosgss.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>

#include <ruby.h>

/* This is allocated in gss_api_context.c. */
extern VALUE e_GssError;

static void raise_gss_error(OM_uint32 err_maj, OM_uint32 err_min)
{
    OM_uint32 maj_stat, min_stat;
    OM_uint32 msg_ctx = 0;
    gss_buffer_desc status_string;
    char buf_maj[512];
    char buf_min[512];

    do {
        maj_stat = gss_display_status(
            &min_stat,
            err_maj,
            GSS_C_GSS_CODE,
            GSS_C_NO_OID,
            &msg_ctx,
            &status_string
        );
        if (GSS_ERROR(maj_stat)) {
            break;
        }
        strncpy(buf_maj, (char*) status_string.value, sizeof(buf_maj));
        gss_release_buffer(&min_stat, &status_string);

        if (!err_min) {
            // There is no minor error to report.
            break;
        }

        maj_stat = gss_display_status(
            &min_stat,
            err_min,
            GSS_C_MECH_CODE,
            GSS_C_NULL_OID,
            &msg_ctx,
            &status_string
        );
        if (! GSS_ERROR(maj_stat)) {
            strncpy(buf_min, (char*) status_string.value, sizeof(buf_min));
            gss_release_buffer(&min_stat, &status_string);
        }
    } while (!GSS_ERROR(maj_stat) && msg_ctx != 0);

    if (err_min) {
        rb_raise(e_GssError, "%s: %s", buf_maj, buf_min);
    } else {
        rb_raise(e_GssError, "%s", buf_maj);
    }
}

void clear_response(gss_client_state* state)
{
    if (state->response != NULL) {
        free(state->response);
        state->response = NULL;
        state->responseLen = 0;
        state->responseConf = 0;
    }
}

static void save_response(gss_client_state* state, gss_buffer_desc* output_token, int conf)
{
    OM_uint32 min_stat, maj_stat;

    if (output_token->length) {
        state->response = malloc(output_token->length);
        memcpy(state->response, output_token->value, output_token->length);
        state->responseLen = output_token->length;
		state->responseConf = conf;

        maj_stat = gss_release_buffer(&min_stat, output_token);
    }
}

static void copy_challenge_to_token(const char* challenge, int challenge_len, gss_buffer_desc* token)
{
    if (challenge && *challenge) {
        // It would be great to just send the challenge along straight, but
        // token->value is a non-const void*, so gss_* functions might
        // mutate it.

        token->value = malloc(challenge_len);
        memcpy(token->value, challenge, challenge_len);
        token->length = challenge_len;
    }
}

void authenticate_gss_client_init(
    const char* service, const char* principal, long int gss_flags,
    gss_server_state* delegatestate, gss_OID mech_oid, gss_client_state* state
)
{
    OM_uint32 maj_stat;
    OM_uint32 min_stat;
    gss_buffer_desc name_token = GSS_C_EMPTY_BUFFER;
    gss_buffer_desc principal_token = GSS_C_EMPTY_BUFFER;

    state->server_name = GSS_C_NO_NAME;
    state->mech_oid = mech_oid;
    state->context = GSS_C_NO_CONTEXT;
    state->gss_flags = gss_flags;
    state->client_creds = GSS_C_NO_CREDENTIAL;
    state->username = NULL;
    clear_response(state);

    // Import server name first
    name_token.length = strlen(service);
    name_token.value = (char *)service;

    maj_stat = gss_import_name(
        &min_stat, &name_token, gss_krb5_nt_service_name, &state->server_name
    );

    if (GSS_ERROR(maj_stat)) {
        raise_gss_error(maj_stat, min_stat);
    }

    // Use the delegate credentials if they exist
    if (delegatestate && delegatestate->client_creds != GSS_C_NO_CREDENTIAL) {
        state->client_creds = delegatestate->client_creds;
    }
    // If available use the principal to extract its associated credentials
    else if (principal && *principal) {
        gss_name_t name;
        principal_token.length = strlen(principal);
        principal_token.value = (char *)principal;

        maj_stat = gss_import_name(
            &min_stat, &principal_token, GSS_C_NT_USER_NAME, &name
        );
        if (GSS_ERROR(maj_stat)) {
            raise_gss_error(maj_stat, min_stat);
        }

        maj_stat = gss_acquire_cred(
            &min_stat, name, GSS_C_INDEFINITE, GSS_C_NO_OID_SET,
            GSS_C_INITIATE, &state->client_creds, NULL, NULL
        );
        if (GSS_ERROR(maj_stat)) {
            raise_gss_error(maj_stat, min_stat);
        }

        maj_stat = gss_release_name(&min_stat, &name);
        if (GSS_ERROR(maj_stat)) {
            raise_gss_error(maj_stat, min_stat);
        }
    }
}

int authenticate_gss_client_clean(gss_client_state *state)
{
    OM_uint32 maj_stat;
    OM_uint32 min_stat;
    int ret = AUTH_GSS_COMPLETE;

    if (state->context != GSS_C_NO_CONTEXT) {
        maj_stat = gss_delete_sec_context(
            &min_stat, &state->context, GSS_C_NO_BUFFER
        );
    }
    if (state->server_name != GSS_C_NO_NAME) {
        maj_stat = gss_release_name(&min_stat, &state->server_name);
    }
    if (
        state->client_creds != GSS_C_NO_CREDENTIAL &&
        ! (state->gss_flags & GSS_C_DELEG_FLAG)
    ) {
        maj_stat = gss_release_cred(&min_stat, &state->client_creds);
    }
    if (state->username != NULL) {
        free(state->username);
        state->username = NULL;
    }
    clear_response(state);

    return ret;
}

int authenticate_gss_client_step(
    gss_client_state* state, const char* challenge, int challenge_len
) {
    OM_uint32 maj_stat;
    OM_uint32 min_stat;
    gss_buffer_desc input_token = GSS_C_EMPTY_BUFFER;
    gss_buffer_desc output_token = GSS_C_EMPTY_BUFFER;
    int ret = AUTH_GSS_CONTINUE;

    // Always clear out the old response
    clear_response(state);

    // If there is a challenge (data from the server) we need to give it to GSS
    copy_challenge_to_token(challenge, challenge_len, &input_token);

    // Do GSSAPI step
    maj_stat = gss_init_sec_context(
        &min_stat,
        state->client_creds,
        &state->context,
        state->server_name,
        state->mech_oid,
        (OM_uint32)state->gss_flags,
        0,
        GSS_C_NO_CHANNEL_BINDINGS,
        &input_token,
        NULL,
        &output_token,
        NULL,
        NULL
    );

    if ((maj_stat != GSS_S_COMPLETE) && (maj_stat != GSS_S_CONTINUE_NEEDED)) {
        if (output_token.value) {
            gss_release_buffer(&min_stat, &output_token);
        }
        if (input_token.value) {
            free(input_token.value);
        }
        raise_gss_error(maj_stat, min_stat);
    }

    ret = (maj_stat == GSS_S_COMPLETE) ? AUTH_GSS_COMPLETE : AUTH_GSS_CONTINUE;

    // Grab the client response to send back to the server
    save_response(state, &output_token, 0);

    // Try to get the user name if we have completed all GSS operations
    if (ret == AUTH_GSS_COMPLETE) {
        gss_name_t gssuser = GSS_C_NO_NAME;
        maj_stat = gss_inquire_context(&min_stat, state->context, &gssuser, NULL, NULL, NULL,  NULL, NULL, NULL);
        if (GSS_ERROR(maj_stat)) {
            if (input_token.value) {
                free(input_token.value);
            }
            if (output_token.value) {
                gss_release_buffer(&min_stat, &output_token);
            }
            raise_gss_error(maj_stat, min_stat);
        }

        gss_buffer_desc name_token;
        name_token.length = 0;
        maj_stat = gss_display_name(&min_stat, gssuser, &name_token, NULL);
        if (GSS_ERROR(maj_stat)) {
            if (name_token.value) {
                gss_release_buffer(&min_stat, &name_token);
            }
            gss_release_name(&min_stat, &gssuser);

            if (input_token.value) {
                free(input_token.value);
            }
            if (output_token.value) {
                gss_release_buffer(&min_stat, &output_token);
            }
            raise_gss_error(maj_stat, min_stat);
        } else {
            if (state->username != NULL) {
                free(state->username);
                state->username = NULL;
            }
            state->username = (char *)malloc(name_token.length + 1);
            strncpy(state->username, (char*) name_token.value, name_token.length);
            state->username[name_token.length] = 0;
            gss_release_buffer(&min_stat, &name_token);
            gss_release_name(&min_stat, &gssuser);
        }
    }

end:
    if (output_token.value) {
        gss_release_buffer(&min_stat, &output_token);
    }
    if (input_token.value) {
        free(input_token.value);
    }
    return ret;
}

int authenticate_gss_client_unwrap(
    gss_client_state *state, const char *challenge, int challenge_len
) {
	OM_uint32 maj_stat;
	OM_uint32 min_stat;
	gss_buffer_desc input_token = GSS_C_EMPTY_BUFFER;
	gss_buffer_desc output_token = GSS_C_EMPTY_BUFFER;
	int ret = AUTH_GSS_CONTINUE;
	int conf = 0;

	// Always clear out the old response
	clear_response(state);

	// If there is a challenge (data from the server) we need to give it to GSS
    copy_challenge_to_token(challenge, challenge_len, &input_token);

	// Do GSSAPI step
	maj_stat = gss_unwrap(
        &min_stat,
        state->context,
        &input_token,
        &output_token,
        &conf,
        NULL
    );

	if (maj_stat != GSS_S_COMPLETE)	{
        if (output_token.value) {
            gss_release_buffer(&min_stat, &output_token);
        }
        if (input_token.value) {
            free(input_token.value);
        }
		raise_gss_error(maj_stat, min_stat);
	} else {
		ret = AUTH_GSS_COMPLETE;
    }

	// Grab the client response
	save_response(state, &output_token, conf);

end:
	if (output_token.value) {
		gss_release_buffer(&min_stat, &output_token);
    }
	if (input_token.value) {
		free(input_token.value);
    }
	return ret;
}

int authenticate_gss_client_wrap(
    gss_client_state* state, const char* challenge, int challenge_len
) {
	OM_uint32 maj_stat;
	OM_uint32 min_stat;
	gss_buffer_desc input_token = GSS_C_EMPTY_BUFFER;
	gss_buffer_desc output_token = GSS_C_EMPTY_BUFFER;
	int ret = AUTH_GSS_CONTINUE;
	char buf[4096], server_conf_flags;
	unsigned long buf_size;

	// Always clear out the old response
	clear_response(state);

    copy_challenge_to_token(challenge, challenge_len, &input_token);

	// Do GSSAPI wrap
	maj_stat = gss_wrap(
        &min_stat,
        state->context,
        0,
        GSS_C_QOP_DEFAULT,
        &input_token,
        NULL,
        &output_token
    );

	if (maj_stat != GSS_S_COMPLETE)	{
        if (output_token.value) {
            gss_release_buffer(&min_stat, &output_token);
        }
		raise_gss_error(maj_stat, min_stat);
	} else {
		ret = AUTH_GSS_COMPLETE;
    }

	// Grab the client response to send back to the server
	save_response(state, &output_token, 0);

end:
	if (output_token.value) {
		gss_release_buffer(&min_stat, &output_token);
    }
	return ret;
}
