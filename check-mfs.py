#!/usr/bin/env python2

import optparse
import socket
import sys
import time
from pprint import pprint
from optparse import OptionParser

try:
    import moosefs
except ImportError:
    print '\nError al importar moosefs.py, verificar instalacion\n'
    sys.exit(1)

parser = OptionParser(usage='%prog [ options ] mfsmaster port ( 9421 | 9413 | 9423 )')
parser.add_option("-i", "--info",    dest='info',    default=False, action='store_true', help="show info about MFS cluster (default if no option given)")
parser.add_option("-d", "--disks",   dest='disks',   default=False, action='store_true', help="show info about disks")
parser.add_option("-e", "--exports", dest='exports', default=False, action='store_true', help="show info about exports")
parser.add_option("-m", "--mountl",  dest='mountl',  default=False, action='store_true', help="show info about mountl")
parser.add_option("-M", "--mounts",  dest='mounts',  default=False, action='store_true', help="show info about mounts")
parser.add_option("-o", "--ops",     dest='ops',     default=False, action='store_true', help="show info about operations")
parser.add_option("-s", "--servers", dest='servers', default=False, action='store_true', help="show info about servers")

(options, args) = parser.parse_args()

if len(args) != 2:
    parser.print_help()
    sys.exit(1)

master = args[0]
port = int(args[1])

try:
    mymfs = moosefs.MooseFS(masterhost=master, masterport=port)
except socket.error, e:
    print '\nError de conexion: %s\n' % str(e)
    sys.exit(1)
except RuntimeError, e:
    print '\nError de conexion: %s\n' % str(e)
    sys.exit(1)


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
        print '%-10s %-25s %2d%% usado - %-25s - %s' % (key.split(':')[0], key.split(':')[1], csd[key][0], csd[key][1], csd[key][2])


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
#    print
#    print myinfo['check_info']
#    print

    masterinfo = myinfo['info']
    matrixinfo = myinfo['matrix']
    chunk_info = myinfo['chunk_info']
    check_info = myinfo['check_info']

    print '\nInformacion del sistema:'
    print 'Archivos:              %10d' % (masterinfo['files'])
    print 'Chunks:                %10d' % (masterinfo['chunks'])
    print 'Chunks sin copias:     %10d' % (sum([matrixinfo[x][0] for x in range(1,5)]))
    print 'Chunks undergoal:      %10d' % (sum([matrixinfo[x][1] for x in range(1,5)]))
#    print 'Chunks undergoal:      %10d' % (check_info['under_goal_chunks']) ## Info desactualizada?
    print 'Chunks overgoal:       %10d' % (sum([matrixinfo[x][3] for x in range(1,5)]))
    print 'Listos p/borrar:       %10d' % (matrixinfo[0][0])
    print 'Pendientes de borrado: %10d' % (sum(matrixinfo[0][1:3]))
    for i in range(len(matrixinfo[0])):
        if matrixinfo[0][i]:
            print 'Pendientes con %d copias: %8d' % (i, matrixinfo[0][i])
    print
    print 'Espacio total:    %6d GB' % (masterinfo['total_space']/1024/1024/1024)
    print 'Disponible:       %6d GB' % (masterinfo['avail_space']/1024/1024/1024)
    print 'Memoria usada:    %6d MB' % (masterinfo['memusage']/1024/1024)

#    print ': %8d' % (matrixinfo[''])
#    print ': %8d' % (chunk_info[''])


def mountl():
    mymountl = mymfs.mfs_mountl()
    print mymountl


def mounts():
    mymounts = mymfs.mfs_mounts()
    for mount in mymounts:
        print '%-12s - %-15s - %-8s - %-20s - %s' % (mount['host'].split('.')[0], mount['ip'], mount['version'], mount['mount'], mount['moose_path'])


def ops():
    myops = mymfs.mfs_operations()
    print myops[0]


def servers():
    myservers = mymfs.mfs_servers()
    a = []

    print '\nMetadata servers:'
    for ml in myservers['metadata_backup_loggers']:
        print ' - %-23s | IP: %12s | version: %12s |' % (ml)

    for cs in myservers['servers']:
        try:
            pu = float(cs['percent_used'])
        except ValueError:
            pu = 0
        if cs['version'] == '256.0.0':
            version = 'disconnected'
        else:
            version = cs['version']
        a.append((cs['host'], cs['ip'], version, pu))
    a.sort()

    print '\nChunk servers:'
    for cs in a:
        print ' - %-23s | IP: %12s | version: %12s | %5.2f percent used' % (cs)

version()

if not (options.info or options.disks or options.exports or options.mountl or options.mounts or options.ops or options.servers):
    info()
if options.info:
    info()
if options.disks:
    disks()
if options.exports:
    exports()
if options.mountl:
    mountl()
if options.mounts:
    mounts()
if options.ops:
    ops()
if options.servers:
    servers()

print
