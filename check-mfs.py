#!/usr/bin/env python2

import sys
import time
from pprint import pprint

try:
    import moosefs
except ImportError:
    print '\nError al importar moosefs.py, verificar instalacion\n'
    sys.exit(1)

if len(sys.argv) != 3:
    print '   Uso: %s mfsmaster puerto (9421 | 9413 | 9423)\n' % sys.argv[0]
    sys.exit(1)

master = sys.argv[1]
port = int(sys.argv[2])

mymfs = moosefs.MooseFS(masterhost=master, masterport=port)


def version():
    mfsversion = mymfs.check_master_version()
    print '\nMaster version: %s' % ('.'.join(str(n) for n in mfsversion))


def disks():
    chunkservers = mymfs.mfs_disks()
    csd = {}
    for cs in chunkservers:
#        host = cs['host_path'].split(':')[0].replace('.linux.backend', '')
#        ruta = cs['host_path'].split(':')[2]
        host = cs['host_path'].split(':')[0].replace('.linux.backend', '') + ':' + cs['host_path'].split(':')[2]
        erro = cs['lerror']
        stat = cs['status']
        perc = int(cs['percent_used'])
        if not isinstance(erro, str):
            erro = time.asctime(erro)
#        csd[host] = (ruta, perc, erro, stat)
        csd[host] = (perc, erro, stat)
    print '\nEstado de los discos:'
    for key in sorted(csd.iterkeys()):
        print '%-10s %-22s %d%% usado - %s - %s' % (key.split(':')[0], key.split(':')[1], csd[key][0], csd[key][1], csd[key][2])


def exports():
    myexports = mymfs.mfs_exports()
    print '\nExports:'
    print '%-15s - %-15s - %s' % ('Desde', 'Hasta', 'Path')
    for export in myexports:
        if export['meta'] == 0:
            print '%-15s - %-15s - %s' % (export['ip_range_from'], export['ip_range_to'], export['path'])


def info():
    myinfo = mymfs.mfs_info()

#    print myinfo['info']
#    print
#    print myinfo['matrix']
#    print
#    print myinfo['chunk_info']

    print '\nInformacion del sistema:'

    masterinfo = myinfo['info']
    matrixinfo = myinfo['matrix']
    chunk_info = myinfo['chunk_info']
    check_info = myinfo['check_info']

    print 'Archivos:         %8d' % (masterinfo['files'])
    print 'Chunks:           %8d' % (masterinfo['chunks'])
    print 'Chunks undergoal: %8d' % (chunk_info['replications_under_goal_out_of'])
#    print 'Chunks undergoal: %8d' % (check_info['under_goal_chunks']) ## Info desactualizada?
    print 'Espacio total:    %5d GB' % (masterinfo['total_space']/1024/1024/1024)
    print 'Disponible:       %5d GB' % (masterinfo['avail_space']/1024/1024/1024)
    print 'Memoria usada:    %5d MB' % (masterinfo['memusage']/1024/1024)
#    print ': %8d' % (matrixinfo[''])
#    print ': %8d' % (chunk_info[''])


def mountl():
    mymountl = mymfs.mfs_mountl()
    print mymountl


def mounts():
    mymounts = mymfs.mfs_mounts()
    print mymounts[0]


def ops():
    myops = mymfs.mfs_operations()
    print myops[0]


def servers():
    myservers = mymfs.mfs_servers()
    print '\nMetadata servers:'
    for ml in myservers['metadata_backup_loggers']:
        print ml
    print '\nChunk servers:'
    for cs in myservers['servers']:
        print cs


version()
#info()
#disks()
#exports()

#mountl()
#mounts()
#ops()
servers()

print
