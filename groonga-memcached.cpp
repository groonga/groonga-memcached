// % g++ -O0 -ggdb3 -I libuv/include -L libuv $(pkg-config --cflags --libs groonga) -o groonga-memcached{,.cpp} -luv
//
// Copyright (C) 2013  Kouhei Sutou <kou@clear-code.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

#include <groonga.h>

#include <uv.h>

#include <string.h>
#include <stdlib.h>

#include <string>
#include <sstream>
#include <iostream>

class Server;
class Connection;

namespace server_callbacks {
  void on_new_connection(uv_stream_t *server_stream, int status);
}

namespace connection_callbacks {
  uv_buf_t allocate_buffer(uv_handle_t *handle, size_t suggested_size);
  void on_read(uv_stream_t *stream, ssize_t n_read, uv_buf_t buffer);
  void on_write(uv_write_t *request, int status);
  void on_close(uv_handle_t *handle);
}


class Server {
public:
  Server() :
    loop_(NULL),
    ipv4_listen_address_("0.0.0.0"),
    ipv6_listen_address_("::0"),
    port_(11211),
    grn_ctx_(grn_ctx_open(0)),
    grn_db_(NULL) {
  }

  ~Server() {
    set_loop(NULL);
    if (grn_db_) {
      grn_obj_close(grn_ctx_, grn_db_);
    }
    grn_ctx_close(grn_ctx_);
  }

  bool parse_arguments(int argc, char **argv) {
    if (argc != 2) {
      std::cerr << "Usage: " << argv[0] << " database [OPTIONS]" << std::endl;
      return false;
    }
    // TODO: check return value
    open_database(argv[1]);
    return true;
  }

  void open_database(const char *path) {
    // TODO: check return value
    grn_db_ = grn_db_open(grn_ctx_, path);
  }

  void run() {
    uv_loop_t *loop = loop_;
    if (!loop) {
      loop = uv_default_loop();
    }
    uv_tcp_t server;
    uv_tcp_init(loop, &server);
    server.data = this;
    bind_ipv4(loop, &server);
    bind_ipv6(loop, &server);
    uv_listen(reinterpret_cast<uv_stream_t *>(&server), 128,
              server_callbacks::on_new_connection);
    uv_run(loop);
  }

  void set_loop(uv_loop_t *loop) {
    if (loop_) {
      uv_loop_delete(loop_);
    }
    loop_ = loop;
  }

  grn_obj *get_grn_db() {
    return grn_db_;
  }

private:
  uv_loop_t *loop_;
  std::string ipv4_listen_address_;
  std::string ipv6_listen_address_;
  int port_;
  grn_ctx *grn_ctx_;
  grn_obj *grn_db_;

  void bind_ipv4(uv_loop_t *loop, uv_tcp_t *tcp) {
    struct sockaddr_in bind_addr = uv_ip4_addr(ipv4_listen_address_.c_str(),
                                               port_);
    uv_tcp_bind(tcp, bind_addr);
  }

  void bind_ipv6(uv_loop_t *loop, uv_tcp_t *tcp) {
    struct sockaddr_in6 bind_addr6 = uv_ip6_addr(ipv6_listen_address_.c_str(),
                                                 port_);
    uv_tcp_bind6(tcp, bind_addr6);
  }
};

class Token {
public:
  Token()
    : data_(NULL),
      length_(0) {
  }

  ~Token() {
  }

  void assign(const char *data, size_t length) {
    data_ = data;
    length_ = length;
  }

  void get_value(const char **data, size_t *length) {
    *data = data_;
    *length = length_;
  }

  bool equal(const char *data) {
    return length_ == strlen(data) && memcmp(data_, data, length_) == 0;
  }

  bool equal(const char *data, size_t length) {
    return length_ == length && memcmp(data_, data, length_) == 0;
  }

private:
  const char *data_;
  size_t length_;
};

class WriteData {
public:
  WriteData(Connection *connection, std::string data)
    : connection_(connection),
      data_(NULL),
      data_length_(0) {
    data_length_ = data.length();
    data_ = static_cast<char *>(malloc(data_length_));
    memcpy(data_, data.data(), data_length_);
  }

  ~WriteData() {
    free(data_);
  }

  Connection *get_connection() {
    return connection_;
  }

  uv_buf_t get_buffer() {
    return uv_buf_init(data_, data_length_);
  }

private:
  Connection *connection_;
  char *data_;
  size_t data_length_;
};

class Connection {
public:
  Connection(Server *server)
    : server_(server),
      client_tcp_(),
      buffer_(""),
      not_processed_data_(""),
      grn_ctx_(grn_ctx_open(0)) {
    grn_ctx_use(grn_ctx_, server->get_grn_db());
  }
  ~Connection() {
    grn_ctx_close(grn_ctx_);
  }

  void on_connect(uv_stream_t *server_stream) {
    uv_tcp_init(server_stream->loop, &client_tcp_);
    client_tcp_.data = this;
    uv_stream_t *client_stream = reinterpret_cast<uv_stream_t *>(&client_tcp_);
    if (uv_accept(server_stream, client_stream) == 0) {
      uv_read_start(client_stream,
                    connection_callbacks::allocate_buffer,
                    connection_callbacks::on_read);
    } else {
      uv_handle_t *client_handle = reinterpret_cast<uv_handle_t *>(&client_tcp_);
      uv_close(client_handle, connection_callbacks::on_close);
    }
  }

  uv_buf_t allocate_buffer(size_t suggested_size) {
    if (suggested_size > buffer_.capacity()) {
      buffer_.reserve(suggested_size);
    }
    return uv_buf_init(const_cast<char *>(buffer_.data()), buffer_.capacity());
  }

  void on_read(uv_stream_t *client_stream, ssize_t n_read, uv_buf_t buffer) {
    if (n_read == -1) {
      uv_handle_t *client_handle =
        reinterpret_cast<uv_handle_t *>(client_stream);
      uv_close(client_handle, connection_callbacks::on_close);
    } else {
      parse_commands(buffer.base, n_read);
    }
  }

  void on_write(uv_write_t *request, int status) {
    WriteData *write_data = static_cast<WriteData *>(request->data);
    delete write_data;
    delete request;
  }

  void on_close(uv_handle_t *handle) {
  }

private:
  Server *server_;
  uv_tcp_t client_tcp_;
  std::string buffer_;
  std::string not_processed_data_;
  grn_ctx *grn_ctx_;

  void parse_commands(const char *data, ssize_t data_length) {
    const char *not_processed_data = data;
    ssize_t not_processed_data_lenegth = data_length;
    for (ssize_t i = 0; i < data_length; i++) {
      if (data[i] == '\n') {
        size_t line_length = i;
        if (i > 0 && data[i - 1] == '\r') {
          line_length--;
        }
        not_processed_data_.append(data, line_length);
        process_command_line(not_processed_data_);
        not_processed_data_.assign("");
        not_processed_data = data + i + 1;
        not_processed_data_lenegth = data_length - i - 1;
      }
    }
    not_processed_data_.append(not_processed_data, not_processed_data_lenegth);
  }

  // The max number of tokens in a command line:
  //   cas <key> <flags> <exptime> <bytes> <cas unique> [noreply]\r\n
  // TODO: support "gets <key>*". It uses variable length arguments.
  static const int MAX_N_COMMAND_LINE_TOKENS = 7;
  void process_command_line(const std::string &line) {
    Token tokens[MAX_N_COMMAND_LINE_TOKENS];
    tokenize_command_line(line, tokens);
    if (tokens[0].equal("get")) {
      process_get(tokens);
    }
  }

  void tokenize_command_line(const std::string &line, Token *tokens) {
    const char *c_line = line.data();
    size_t line_length = line.length();
    const char *token = c_line;
    int token_index = 0;

    for (size_t i = 0; i < line_length; i++) {
      if (c_line[i] != ' ') {
        continue;
      }

      if (i > 0) {
        tokens[token_index].assign(token, i - (token - c_line));
        token_index++;
        if (token_index >= MAX_N_COMMAND_LINE_TOKENS) {
          return;
        }
      }
      for (; i < line_length; i++) {
        if (c_line[i] != ' ') {
          break;
        }
      }
      token = c_line + i;
    }

    if (c_line[line_length] != ' ') {
      tokens[token_index].assign(token, line_length - (token - c_line));
    }
  }

  // The encoded key format:
  //   <table>.<column>:<key>
  static const int MAX_N_KEY_COMPONENTS = 3;
  void process_get(Token *tokens) {
    const char *encoded_key;
    size_t encoded_key_length;
    tokens[1].get_value(&encoded_key, &encoded_key_length);

    Token key_components[MAX_N_KEY_COMPONENTS];
    decode_key(encoded_key, encoded_key_length, key_components);

    const char *table_name;
    size_t table_name_length;
    key_components[0].get_value(&table_name, &table_name_length);

    const char *column_name;
    size_t column_name_length;
    key_components[1].get_value(&column_name, &column_name_length);

    const char *key;
    size_t key_length;
    key_components[2].get_value(&key, &key_length);

    // TODO: check return value
    grn_obj *table = grn_ctx_get(grn_ctx_, table_name, table_name_length);
    // TODO: check return value
    grn_obj *column = grn_obj_column(grn_ctx_, table,
                                     column_name, column_name_length);
    // TODO: check return value
    grn_id id = grn_table_get(grn_ctx_, table, key, key_length);
    grn_obj value;
    GRN_TEXT_INIT(&value, 0);
    grn_obj_get_value(grn_ctx_, column, id, &value);
    write_get_response(key, key_length, &value);
  }

  void write_get_response(const char *key, size_t key_length, grn_obj *value) {
    std::ostringstream response_stream;
    int flags = 0;
    response_stream << "VALUE ";
    response_stream.write(key, key_length);
    response_stream << " " << flags << " " << GRN_TEXT_LEN(value) << "\r\n";
    response_stream.write(GRN_TEXT_VALUE(value), GRN_TEXT_LEN(value));
    response_stream << "\r\n";
    response_stream << "END\r\n";

    uv_write_t *request = new uv_write_t();
    WriteData *write_data = new WriteData(this, response_stream.str());
    request->data = write_data;
    uv_stream_t *client_stream = reinterpret_cast<uv_stream_t *>(&client_tcp_);
    uv_buf_t response_buffer = write_data->get_buffer();
    uv_write(request, client_stream, &response_buffer, 1,
             connection_callbacks::on_write);
  }

  void decode_key(const char *encoded_key, size_t encoded_key_length,
                  Token *key_components) {
    size_t i;

    for (i = 0; i < encoded_key_length; i++) {
      if (encoded_key[i] == '.') {
        key_components[0].assign(encoded_key, i);
        i++;
        break;
      }
    }

    size_t column_name_start = i;
    for (; i < encoded_key_length; i++) {
      if (encoded_key[i] == ':') {
        key_components[1].assign(encoded_key + column_name_start,
                                 i - column_name_start);
        key_components[2].assign(encoded_key + i + 1,
                                 encoded_key_length - i - 1);
        break;
      }
    }
  }
};

namespace server_callbacks {
  void on_new_connection(uv_stream_t *server_stream, int status) {
    if (status == -1) {
      // error!
      return;
    }

    Server *server = static_cast<Server *>(server_stream->data);
    Connection *connection = new Connection(server);
    connection->on_connect(server_stream);
  }
}

namespace connection_callbacks {
  uv_buf_t allocate_buffer(uv_handle_t *handle, size_t suggested_size) {
    Connection *connection = static_cast<Connection *>(handle->data);
    return connection->allocate_buffer(suggested_size);
  }

  void on_read(uv_stream_t *stream, ssize_t n_read, uv_buf_t buffer) {
    Connection *connection = static_cast<Connection *>(stream->data);
    connection->on_read(stream, n_read, buffer);
  }

  void on_write(uv_write_t *request, int status) {
    WriteData *write_data = static_cast<WriteData *>(request->data);
    Connection *connection = write_data->get_connection();
    connection->on_write(request, status);
  }

  void on_close(uv_handle_t *handle) {
    Connection *connection = static_cast<Connection *>(handle->data);
    connection->on_close(handle);
    delete connection;
  }
}

int run_server(int argc, char **argv) {
  Server server;

  if (!server.parse_arguments(argc, argv)) {
    return EXIT_FAILURE;
  }

  server.run();
  return EXIT_SUCCESS;
}

int main(int argc, char **argv) {
  grn_init();
  int exit_code = run_server(argc, argv);
  grn_fin();

  return exit_code;
}
