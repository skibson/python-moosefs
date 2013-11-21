#!/usr/bin/python

import sys
import socket

try:
    import collectd
except ImportError:
    pass

try:
    import moosefs
except ImportError:
    print '\nError al importar moosefs, verificar instalacion\n'
    sys.exit(2)

master = ''
port = 9421

def get_chunks(master, port=9421):
    chunks = {}
    try:
        mymfs = moosefs.MooseFS(masterhost=master, masterport=port)
    except socket.error, e:
        print '\nError de conexion: %s\n' % str(e)
        chunks['sin_copias'] = 0
        chunks['menos_copias'] = 0
        chunks['a_borrar'] = 0
        return chunks

    myinfo = mymfs.mfs_info()
    masterinfo = myinfo['info']
    matrixinfo = myinfo['matrix']
    chunk_info = myinfo['chunk_info']

    missing = int(sum([matrixinfo[x][0] for x in range(1,5)]))
    undergoal = int(chunk_info['replications_under_goal_out_of'])
    pending = int(matrixinfo[0][0])
    ready = sum(matrixinfo[0][1:3])

#    for i in range(len(matrixinfo[0])):
#        if matrixinfo[0][i]:
#            print 'Pendientes con %d copias: %8d' % (i, matrixinfo[0][i])

    chunks['missing'] = missing
    chunks['undergoal'] = undergoal
    chunks['pending'] = pending
    chunks['ready'] = ready
    return chunks


def configure(data=None):
    global master, port
    for node in data.children:
        if node.key == 'master':
            master = node.values[0]
        elif node.key == 'port':
            port = int(node.values[0])
    try:
        mymfs = moosefs.MooseFS(masterhost=master, masterport=port)
    except socket.error, e:
        print '\nError de conexion: %s\n' % str(e)


def read_mfs(data=None):
    global master, port
    chunks = get_chunks(master, port)
    vl = collectd.Values(type='gauge', plugin='chunks')
    for chunk in chunks:
        vl.plugin_instance = chunk
        vl.type_instance = 'cant_chunks'
        vl.values = [ int(chunks[chunk]) ]
        vl.dispatch()


try:
    collectd.register_config(configure)
    collectd.register_read(read_mfs)
except NameError:
    pass

if __name__ == "__main__":
    print get_chunks(sys.argv[1], int(sys.argv[2]))

