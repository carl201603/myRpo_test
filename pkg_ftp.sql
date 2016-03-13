CREATE OR REPLACE PACKAGE pkg_ftp AS
  TYPE t_string_table IS TABLE OF VARCHAR2(32767);
  FUNCTION login(p_host    IN VARCHAR2,
                 p_port    IN VARCHAR2,
                 p_user    IN VARCHAR2,
                 p_pass    IN VARCHAR2,
                 p_timeout IN NUMBER := NULL) RETURN utl_tcp.connection;
  FUNCTION get_passive(p_conn IN OUT NOCOPY utl_tcp.connection)
    RETURN utl_tcp.connection;
  PROCEDURE logout(p_conn  IN OUT NOCOPY utl_tcp.connection,
                   p_reply IN BOOLEAN := TRUE);
  PROCEDURE send_command(p_conn    IN OUT NOCOPY utl_tcp.connection,
                         p_command IN VARCHAR2,
                         p_reply   IN BOOLEAN := TRUE);
  PROCEDURE get_reply(p_conn IN OUT NOCOPY utl_tcp.connection);
  FUNCTION get_local_ascii_data(p_dir IN VARCHAR2, p_file IN VARCHAR2)
    RETURN CLOB;
  FUNCTION get_local_binary_data(p_dir IN VARCHAR2, p_file IN VARCHAR2)
    RETURN BLOB;
  FUNCTION get_remote_ascii_data(p_conn IN OUT NOCOPY utl_tcp.connection,
                                 p_file IN VARCHAR2) RETURN CLOB;
  FUNCTION get_remote_binary_data(p_conn IN OUT NOCOPY utl_tcp.connection,
                                  p_file IN VARCHAR2) RETURN BLOB;
  PROCEDURE put_local_ascii_data(p_data IN CLOB,
                                 p_dir  IN VARCHAR2,
                                 p_file IN VARCHAR2);
  PROCEDURE put_local_binary_data(p_data IN BLOB,
                                  p_dir  IN VARCHAR2,
                                  p_file IN VARCHAR2);
  PROCEDURE put_remote_ascii_data(p_conn IN OUT NOCOPY utl_tcp.connection,
                                  p_file IN VARCHAR2,
                                  p_data IN CLOB);
  PROCEDURE put_remote_binary_data(p_conn IN OUT NOCOPY utl_tcp.connection,
                                   p_file IN VARCHAR2,
                                   p_data IN BLOB);
  PROCEDURE get(p_conn      IN OUT NOCOPY utl_tcp.connection,
                p_from_file IN VARCHAR2,
                p_to_dir    IN VARCHAR2,
                p_to_file   IN VARCHAR2);
  PROCEDURE put(p_conn      IN OUT NOCOPY utl_tcp.connection,
                p_from_dir  IN VARCHAR2,
                p_from_file IN VARCHAR2,
                p_to_file   IN VARCHAR2);
  PROCEDURE get_direct(p_conn      IN OUT NOCOPY utl_tcp.connection,
                       p_from_file IN VARCHAR2,
                       p_to_dir    IN VARCHAR2,
                       p_to_file   IN VARCHAR2);
  PROCEDURE put_direct(p_conn      IN OUT NOCOPY utl_tcp.connection,
                       p_from_dir  IN VARCHAR2,
                       p_from_file IN VARCHAR2,
                       p_to_file   IN VARCHAR2);
  PROCEDURE help(p_conn IN OUT NOCOPY utl_tcp.connection);
  PROCEDURE ascii(p_conn IN OUT NOCOPY utl_tcp.connection);
  PROCEDURE binary(p_conn IN OUT NOCOPY utl_tcp.connection);
  PROCEDURE list(p_conn IN OUT NOCOPY utl_tcp.connection,
                 p_dir  IN VARCHAR2,
                 p_list OUT t_string_table);
  PROCEDURE nlst(p_conn IN OUT NOCOPY utl_tcp.connection,
                 p_dir  IN VARCHAR2,
                 p_list OUT t_string_table);
  PROCEDURE rename(p_conn IN OUT NOCOPY utl_tcp.connection,
                   p_from IN VARCHAR2,
                   p_to   IN VARCHAR2);
  PROCEDURE DELETE(p_conn IN OUT NOCOPY utl_tcp.connection,
                   p_file IN VARCHAR2);
  PROCEDURE mkdir(p_conn IN OUT NOCOPY utl_tcp.connection,
                  p_dir  IN VARCHAR2);
  PROCEDURE rmdir(p_conn IN OUT NOCOPY utl_tcp.connection,
                  p_dir  IN VARCHAR2);
  PROCEDURE convert_crlf(p_status IN BOOLEAN);
END pkg_ftp;

/


CREATE OR REPLACE PACKAGE BODY pkg_ftp AS

  g_reply        t_string_table := t_string_table();
  g_binary       BOOLEAN := TRUE;
  g_debug        BOOLEAN := TRUE;
  g_convert_crlf BOOLEAN := TRUE;

  PROCEDURE debug(p_text IN VARCHAR2);

  FUNCTION login(p_host    IN VARCHAR2,
                 p_port    IN VARCHAR2,
                 p_user    IN VARCHAR2,
                 p_pass    IN VARCHAR2,
                 p_timeout IN NUMBER := NULL) RETURN utl_tcp.connection IS
    l_conn utl_tcp.connection;
  BEGIN
    g_reply.delete;
  
    l_conn := utl_tcp.open_connection(p_host,
                                      p_port,
                                      tx_timeout => p_timeout);
    get_reply(l_conn);
    send_command(l_conn, 'USER ' || p_user);
    send_command(l_conn, 'PASS ' || p_pass);
    RETURN l_conn;
  END;

  FUNCTION get_passive(p_conn IN OUT NOCOPY utl_tcp.connection)
    RETURN utl_tcp.connection IS
    l_conn  utl_tcp.connection;
    l_reply VARCHAR2(32767);
    l_host  VARCHAR(100);
    l_port1 NUMBER(10);
    l_port2 NUMBER(10);
  BEGIN
    send_command(p_conn, 'PASV');
    l_reply := g_reply(g_reply.last);
  
    l_reply := REPLACE(substr(l_reply,
                              instr(l_reply, '(') + 1,
                              (instr(l_reply, ')')) - (instr(l_reply, '(')) - 1),
                       ',',
                       '.');
    l_host  := substr(l_reply, 1, instr(l_reply, '.', 1, 4) - 1);
  
    l_port1 := to_number(substr(l_reply,
                                instr(l_reply, '.', 1, 4) + 1,
                                (instr(l_reply, '.', 1, 5) - 1) -
                                (instr(l_reply, '.', 1, 4))));
    l_port2 := to_number(substr(l_reply, instr(l_reply, '.', 1, 5) + 1));
  
    l_conn := utl_tcp.open_connection(l_host, 256 * l_port1 + l_port2);
    RETURN l_conn;
  END;

  PROCEDURE logout(p_conn  IN OUT NOCOPY utl_tcp.connection,
                   p_reply IN BOOLEAN := TRUE) AS
  BEGIN
    send_command(p_conn, 'QUIT', p_reply);
    utl_tcp.close_connection(p_conn);
  END;

  PROCEDURE send_command(p_conn    IN OUT NOCOPY utl_tcp.connection,
                         p_command IN VARCHAR2,
                         p_reply   IN BOOLEAN := TRUE) IS
    l_result PLS_INTEGER;
  BEGIN
    l_result := utl_tcp.write_line(p_conn, p_command);
  
    IF p_reply THEN
      get_reply(p_conn);
    END IF;
  END;

  PROCEDURE get_reply(p_conn IN OUT NOCOPY utl_tcp.connection) IS
    l_reply_code VARCHAR2(3) := NULL;
  BEGIN
    LOOP
      g_reply.extend;
      g_reply(g_reply.last) := utl_tcp.get_line(p_conn, TRUE);
      debug(g_reply(g_reply.last));
      IF l_reply_code IS NULL THEN
        l_reply_code := substr(g_reply(g_reply.last), 1, 3);
      END IF;
      IF substr(l_reply_code, 1, 1) IN ('4', '5') THEN
        raise_application_error(-20000, g_reply(g_reply.last));
      ELSIF (substr(g_reply(g_reply.last), 1, 3) = l_reply_code AND
            substr(g_reply(g_reply.last), 4, 1) = ' ') THEN
        EXIT;
      END IF;
    END LOOP;
  EXCEPTION
    WHEN utl_tcp.end_of_input THEN
      NULL;
  END;

  FUNCTION get_local_ascii_data(p_dir IN VARCHAR2, p_file IN VARCHAR2)
    RETURN CLOB IS
    l_bfile BFILE;
    l_data  CLOB;
  BEGIN
    dbms_lob.createtemporary(lob_loc => l_data,
                             cache   => TRUE,
                             dur     => dbms_lob.call);
  
    l_bfile := bfilename(p_dir, p_file);
    dbms_lob.fileopen(l_bfile, dbms_lob.file_readonly);
  
    IF dbms_lob.getlength(l_bfile) > 0 THEN
      dbms_lob.loadfromfile(l_data, l_bfile, dbms_lob.getlength(l_bfile));
    END IF;
  
    dbms_lob.fileclose(l_bfile);
  
    RETURN l_data;
  END;

  FUNCTION get_local_binary_data(p_dir IN VARCHAR2, p_file IN VARCHAR2)
    RETURN BLOB IS
    l_bfile BFILE;
    l_data  BLOB;
  BEGIN
    dbms_lob.createtemporary(lob_loc => l_data,
                             cache   => TRUE,
                             dur     => dbms_lob.call);
  
    l_bfile := bfilename(p_dir, p_file);
    dbms_lob.fileopen(l_bfile, dbms_lob.file_readonly);
    IF dbms_lob.getlength(l_bfile) > 0 THEN
      dbms_lob.loadfromfile(l_data, l_bfile, dbms_lob.getlength(l_bfile));
    END IF;
    dbms_lob.fileclose(l_bfile);
  
    RETURN l_data;
  END;

  FUNCTION get_remote_ascii_data(p_conn IN OUT NOCOPY utl_tcp.connection,
                                 p_file IN VARCHAR2) RETURN CLOB IS
    l_conn   utl_tcp.connection;
    l_amount PLS_INTEGER;
    l_buffer VARCHAR2(32767);
    l_data   CLOB;
  BEGIN
    dbms_lob.createtemporary(lob_loc => l_data,
                             cache   => TRUE,
                             dur     => dbms_lob.call);
  
    l_conn := get_passive(p_conn);
    send_command(p_conn, 'RETR ' || p_file, TRUE);
  
    BEGIN
      LOOP
        l_amount := utl_tcp.read_text(l_conn, l_buffer, 32767);
        dbms_lob.writeappend(l_data, l_amount, l_buffer);
      END LOOP;
    EXCEPTION
      WHEN utl_tcp.end_of_input THEN
        NULL;
      WHEN OTHERS THEN
        NULL;
    END;
    utl_tcp.close_connection(l_conn);
    get_reply(p_conn);
  
    RETURN l_data;
  
  EXCEPTION
    WHEN OTHERS THEN
      utl_tcp.close_connection(l_conn);
      RAISE;
  END;

  FUNCTION get_remote_binary_data(p_conn IN OUT NOCOPY utl_tcp.connection,
                                  p_file IN VARCHAR2) RETURN BLOB IS
    l_conn   utl_tcp.connection;
    l_amount PLS_INTEGER;
    l_buffer RAW(32767);
    l_data   BLOB;
  BEGIN
    dbms_lob.createtemporary(lob_loc => l_data,
                             cache   => TRUE,
                             dur     => dbms_lob.call);
  
    l_conn := get_passive(p_conn);
    send_command(p_conn, 'RETR ' || p_file, TRUE);
  
    BEGIN
      LOOP
        l_amount := utl_tcp.read_raw(l_conn, l_buffer, 32767);
        dbms_lob.writeappend(l_data, l_amount, l_buffer);
      END LOOP;
    EXCEPTION
      WHEN utl_tcp.end_of_input THEN
        NULL;
      WHEN OTHERS THEN
        NULL;
    END;
    utl_tcp.close_connection(l_conn);
    get_reply(p_conn);
  
    RETURN l_data;
  
  EXCEPTION
    WHEN OTHERS THEN
      utl_tcp.close_connection(l_conn);
      RAISE;
  END;

  PROCEDURE put_local_ascii_data(p_data IN CLOB,
                                 p_dir  IN VARCHAR2,
                                 p_file IN VARCHAR2) IS
    l_out_file utl_file.file_type;
    l_buffer   VARCHAR2(32767);
    l_amount   BINARY_INTEGER := 32767;
    l_pos      INTEGER := 1;
    l_clob_len INTEGER;
  BEGIN
    l_clob_len := dbms_lob.getlength(p_data);
  
    l_out_file := utl_file.fopen(p_dir, p_file, 'w', 32767);
  
    WHILE l_pos <= l_clob_len LOOP
      dbms_lob.read(p_data, l_amount, l_pos, l_buffer);
      IF g_convert_crlf THEN
        l_buffer := REPLACE(l_buffer, chr(13), NULL);
      END IF;
    
      utl_file.put(l_out_file, l_buffer);
      utl_file.fflush(l_out_file);
      l_pos := l_pos + l_amount;
    END LOOP;
  
    utl_file.fclose(l_out_file);
  EXCEPTION
    WHEN OTHERS THEN
      IF utl_file.is_open(l_out_file) THEN
        utl_file.fclose(l_out_file);
      END IF;
      RAISE;
  END;

  PROCEDURE put_local_binary_data(p_data IN BLOB,
                                  p_dir  IN VARCHAR2,
                                  p_file IN VARCHAR2) IS
    l_out_file utl_file.file_type;
    l_buffer   RAW(32767);
    l_amount   BINARY_INTEGER := 32767;
    l_pos      INTEGER := 1;
    l_blob_len INTEGER;
  BEGIN
    l_blob_len := dbms_lob.getlength(p_data);
  
    l_out_file := utl_file.fopen(p_dir, p_file, 'wb', 32767);
  
    WHILE l_pos <= l_blob_len LOOP
      dbms_lob.read(p_data, l_amount, l_pos, l_buffer);
      utl_file.put_raw(l_out_file, l_buffer, TRUE);
      utl_file.fflush(l_out_file);
      l_pos := l_pos + l_amount;
    END LOOP;
  
    utl_file.fclose(l_out_file);
  EXCEPTION
    WHEN OTHERS THEN
      IF utl_file.is_open(l_out_file) THEN
        utl_file.fclose(l_out_file);
      END IF;
      RAISE;
  END;

  PROCEDURE put_remote_ascii_data(p_conn IN OUT NOCOPY utl_tcp.connection,
                                  p_file IN VARCHAR2,
                                  p_data IN CLOB) IS
    l_conn     utl_tcp.connection;
    l_result   PLS_INTEGER;
    l_buffer   VARCHAR2(32767);
    l_amount   BINARY_INTEGER := 32767; -- Switch to 10000 (or use binary) if you get ORA-06502 from this line.
    l_pos      INTEGER := 1;
    l_clob_len INTEGER;
  BEGIN
    l_conn := get_passive(p_conn);
    send_command(p_conn, 'STOR ' || p_file, TRUE);
  
    l_clob_len := dbms_lob.getlength(p_data);
  
    WHILE l_pos <= l_clob_len LOOP
      dbms_lob.read(p_data, l_amount, l_pos, l_buffer);
      IF g_convert_crlf THEN
        l_buffer := REPLACE(l_buffer, chr(13), NULL);
      END IF;
      l_result := utl_tcp.write_text(l_conn, l_buffer, length(l_buffer));
      utl_tcp.flush(l_conn);
      l_pos := l_pos + l_amount;
    END LOOP;
  
    utl_tcp.close_connection(l_conn);
    -- The following line allows some people to make multiple calls from one connection.
    -- It causes the operation to hang for me, hence it is commented out by default.
    -- get_reply(p_conn);
  
  EXCEPTION
    WHEN OTHERS THEN
      utl_tcp.close_connection(l_conn);
      RAISE;
  END;

  PROCEDURE put_remote_binary_data(p_conn IN OUT NOCOPY utl_tcp.connection,
                                   p_file IN VARCHAR2,
                                   p_data IN BLOB) IS
    l_conn     utl_tcp.connection;
    l_result   PLS_INTEGER;
    l_buffer   RAW(32767);
    l_amount   BINARY_INTEGER := 32767;
    l_pos      INTEGER := 1;
    l_blob_len INTEGER;
  BEGIN
    l_conn := get_passive(p_conn);
    send_command(p_conn, 'STOR ' || p_file, TRUE);
  
    l_blob_len := dbms_lob.getlength(p_data);
  
    WHILE l_pos <= l_blob_len LOOP
      dbms_lob.read(p_data, l_amount, l_pos, l_buffer);
      l_result := utl_tcp.write_raw(l_conn, l_buffer, l_amount);
      utl_tcp.flush(l_conn);
      l_pos := l_pos + l_amount;
    END LOOP;
  
    utl_tcp.close_connection(l_conn);
    -- The following line allows some people to make multiple calls from one connection.
    -- It causes the operation to hang for me, hence it is commented out by default.
    -- get_reply(p_conn);
  
  EXCEPTION
    WHEN OTHERS THEN
      utl_tcp.close_connection(l_conn);
      RAISE;
  END;

  PROCEDURE get(p_conn      IN OUT NOCOPY utl_tcp.connection,
                p_from_file IN VARCHAR2,
                p_to_dir    IN VARCHAR2,
                p_to_file   IN VARCHAR2) AS
  BEGIN
    IF g_binary THEN
      put_local_binary_data(p_data => get_remote_binary_data(p_conn,
                                                             p_from_file),
                            p_dir  => p_to_dir,
                            p_file => p_to_file);
    ELSE
      put_local_ascii_data(p_data => get_remote_ascii_data(p_conn,
                                                           p_from_file),
                           p_dir  => p_to_dir,
                           p_file => p_to_file);
    END IF;
  END;

  PROCEDURE put(p_conn      IN OUT NOCOPY utl_tcp.connection,
                p_from_dir  IN VARCHAR2,
                p_from_file IN VARCHAR2,
                p_to_file   IN VARCHAR2) AS
  BEGIN
    IF g_binary THEN
      put_remote_binary_data(p_conn => p_conn,
                             p_file => p_to_file,
                             p_data => get_local_binary_data(p_from_dir,
                                                             p_from_file));
    ELSE
      put_remote_ascii_data(p_conn => p_conn,
                            p_file => p_to_file,
                            p_data => get_local_ascii_data(p_from_dir,
                                                           p_from_file));
    END IF;
    get_reply(p_conn);
  END;

  PROCEDURE get_direct(p_conn      IN OUT NOCOPY utl_tcp.connection,
                       p_from_file IN VARCHAR2,
                       p_to_dir    IN VARCHAR2,
                       p_to_file   IN VARCHAR2) IS
    l_conn       utl_tcp.connection;
    l_out_file   utl_file.file_type;
    l_amount     PLS_INTEGER;
    l_buffer     VARCHAR2(32767);
    l_raw_buffer RAW(32767);
  BEGIN
    l_conn := get_passive(p_conn);
    send_command(p_conn, 'RETR ' || p_from_file, TRUE);
    IF g_binary THEN
      l_out_file := utl_file.fopen(p_to_dir, p_to_file, 'wb', 32767);
    ELSE
      l_out_file := utl_file.fopen(p_to_dir, p_to_file, 'w', 32767);
    END IF;
  
    BEGIN
      LOOP
        IF g_binary THEN
          l_amount := utl_tcp.read_raw(l_conn, l_raw_buffer, 32767);
          utl_file.put_raw(l_out_file, l_raw_buffer, TRUE);
        ELSE
          l_amount := utl_tcp.read_text(l_conn, l_buffer, 32767);
          IF g_convert_crlf THEN
            l_buffer := REPLACE(l_buffer, chr(13), NULL);
          END IF;
          utl_file.put(l_out_file, l_buffer);
        END IF;
        utl_file.fflush(l_out_file);
      END LOOP;
    EXCEPTION
      WHEN utl_tcp.end_of_input THEN
        NULL;
      WHEN OTHERS THEN
        NULL;
    END;
    utl_file.fclose(l_out_file);
    utl_tcp.close_connection(l_conn);
  EXCEPTION
    WHEN OTHERS THEN
      IF utl_file.is_open(l_out_file) THEN
        utl_file.fclose(l_out_file);
      END IF;
      RAISE;
  END;

  PROCEDURE put_direct(p_conn      IN OUT NOCOPY utl_tcp.connection,
                       p_from_dir  IN VARCHAR2,
                       p_from_file IN VARCHAR2,
                       p_to_file   IN VARCHAR2) IS
    l_conn       utl_tcp.connection;
    l_bfile      BFILE;
    l_result     PLS_INTEGER;
    l_amount     PLS_INTEGER := 32767;
    l_raw_buffer RAW(32767);
    l_len        NUMBER;
    l_pos        NUMBER := 1;
    ex_ascii EXCEPTION;
  BEGIN
    IF NOT g_binary THEN
      RAISE ex_ascii;
    END IF;
  
    l_conn := get_passive(p_conn);
    send_command(p_conn, 'STOR ' || p_to_file, TRUE);
  
    l_bfile := bfilename(p_from_dir, p_from_file);
  
    dbms_lob.fileopen(l_bfile, dbms_lob.file_readonly);
    l_len := dbms_lob.getlength(l_bfile);
  
    WHILE l_pos <= l_len LOOP
      dbms_lob.read(l_bfile, l_amount, l_pos, l_raw_buffer);
      debug(l_amount);
      l_result := utl_tcp.write_raw(l_conn, l_raw_buffer, l_amount);
      l_pos    := l_pos + l_amount;
    END LOOP;
  
    dbms_lob.fileclose(l_bfile);
    utl_tcp.close_connection(l_conn);
  EXCEPTION
    WHEN ex_ascii THEN
      raise_application_error(-20000,
                              'PUT_DIRECT not available in ASCII mode.');
    WHEN OTHERS THEN
      IF dbms_lob.fileisopen(l_bfile) = 1 THEN
        dbms_lob.fileclose(l_bfile);
      END IF;
      RAISE;
  END;

  PROCEDURE help(p_conn IN OUT NOCOPY utl_tcp.connection) AS
  BEGIN
    send_command(p_conn, 'HELP', TRUE);
  END;

  PROCEDURE ascii(p_conn IN OUT NOCOPY utl_tcp.connection) AS
  BEGIN
    send_command(p_conn, 'TYPE A', TRUE);
    g_binary := FALSE;
  END;

  PROCEDURE binary(p_conn IN OUT NOCOPY utl_tcp.connection) AS
  BEGIN
    send_command(p_conn, 'TYPE I', TRUE);
    g_binary := TRUE;
  END;

  PROCEDURE list(p_conn IN OUT NOCOPY utl_tcp.connection,
                 p_dir  IN VARCHAR2,
                 p_list OUT t_string_table) AS
    l_conn       utl_tcp.connection;
    l_list       t_string_table := t_string_table();
    l_reply_code VARCHAR2(3) := NULL;
  BEGIN
    l_conn := get_passive(p_conn);
    send_command(p_conn, 'LIST ' || p_dir, TRUE);
  
    BEGIN
      LOOP
        l_list.extend;
        l_list(l_list.last) := utl_tcp.get_line(l_conn, TRUE);
        debug(l_list(l_list.last));
        IF l_reply_code IS NULL THEN
          l_reply_code := substr(l_list(l_list.last), 1, 3);
        END IF;
        IF (substr(l_reply_code, 1, 1) IN ('4', '5') AND
           substr(l_reply_code, 4, 1) = ' ') THEN
          raise_application_error(-20000, l_list(l_list.last));
        ELSIF (substr(g_reply(g_reply.last), 1, 3) = l_reply_code AND
              substr(g_reply(g_reply.last), 4, 1) = ' ') THEN
          EXIT;
        END IF;
      END LOOP;
    EXCEPTION
      WHEN utl_tcp.end_of_input THEN
        NULL;
    END;
  
    l_list.delete(l_list.last);
    p_list := l_list;
  
    utl_tcp.close_connection(l_conn);
    get_reply(p_conn);
  END;

  PROCEDURE nlst(p_conn IN OUT NOCOPY utl_tcp.connection,
                 p_dir  IN VARCHAR2,
                 p_list OUT t_string_table) AS
    l_conn       utl_tcp.connection;
    l_list       t_string_table := t_string_table();
    l_reply_code VARCHAR2(3) := NULL;
  BEGIN
    l_conn := get_passive(p_conn);
    send_command(p_conn, 'NLST ' || p_dir, TRUE);
  
    BEGIN
      LOOP
        l_list.extend;
        l_list(l_list.last) := utl_tcp.get_line(l_conn, TRUE);
        debug(l_list(l_list.last));
        IF l_reply_code IS NULL THEN
          l_reply_code := substr(l_list(l_list.last), 1, 3);
        END IF;
        IF (substr(l_reply_code, 1, 1) IN ('4', '5') AND
           substr(l_reply_code, 4, 1) = ' ') THEN
          raise_application_error(-20000, l_list(l_list.last));
        ELSIF (substr(g_reply(g_reply.last), 1, 3) = l_reply_code AND
              substr(g_reply(g_reply.last), 4, 1) = ' ') THEN
          EXIT;
        END IF;
      END LOOP;
    EXCEPTION
      WHEN utl_tcp.end_of_input THEN
        NULL;
    END;
  
    l_list.delete(l_list.last);
    p_list := l_list;
  
    utl_tcp.close_connection(l_conn);
    get_reply(p_conn);
  END;

  PROCEDURE rename(p_conn IN OUT NOCOPY utl_tcp.connection,
                   p_from IN VARCHAR2,
                   p_to   IN VARCHAR2) AS
    l_conn utl_tcp.connection;
  BEGIN
    l_conn := get_passive(p_conn);
    send_command(p_conn, 'RNFR ' || p_from, TRUE);
    send_command(p_conn, 'RNTO ' || p_to, TRUE);
    logout(l_conn, FALSE);
  END rename;

  PROCEDURE DELETE(p_conn IN OUT NOCOPY utl_tcp.connection,
                   p_file IN VARCHAR2) AS
    l_conn utl_tcp.connection;
  BEGIN
    l_conn := get_passive(p_conn);
    send_command(p_conn, 'DELE ' || p_file, TRUE);
    logout(l_conn, FALSE);
  END DELETE;

  PROCEDURE mkdir(p_conn IN OUT NOCOPY utl_tcp.connection,
                  p_dir  IN VARCHAR2) AS
    l_conn utl_tcp.connection;
  BEGIN
    l_conn := get_passive(p_conn);
    send_command(p_conn, 'MKD ' || p_dir, TRUE);
    logout(l_conn, FALSE);
  END mkdir;

  PROCEDURE rmdir(p_conn IN OUT NOCOPY utl_tcp.connection,
                  p_dir  IN VARCHAR2) AS
    l_conn utl_tcp.connection;
  BEGIN
    l_conn := get_passive(p_conn);
    send_command(p_conn, 'RMD ' || p_dir, TRUE);
    logout(l_conn, FALSE);
  END rmdir;

  PROCEDURE convert_crlf(p_status IN BOOLEAN) AS
  BEGIN
    g_convert_crlf := p_status;
  END;

  PROCEDURE debug(p_text IN VARCHAR2) IS
  BEGIN
    IF g_debug THEN
      dbms_output.put_line(substr(p_text, 1, 255));
    END IF;
  END;

END pkg_ftp;
