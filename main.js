#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const net = require('net');
const ServerNode = require('./server-node.js');
const ClientFuse = require('./client-fuse.js');
const ConfigFile = require('./config-file.js');
const ModFdcache = require('./mod-fdcache.js');
const ModRedundancy = require('./mod-redundancy.js');

// >0 = Numerical result out of range
//  0 = Success
// -1 = Operation not permitted
// -2 = No such file or directory
// -3 = No such process
// -4 = Interrupted system call
// -5 = Input/output error
// -6 = No such device or address
// -7 = Argument list too long
// -8 = Exec format error
// -9 = Bad file descriptor
// -10 = No child processes
// -11 = Resource temporarily unavailable
// -12 = Cannot allocate memory
// -13 = Permission denied
// -14 = Bad address
// -15 = Block device required
// -16 = Devire or resource busy
// -17 = File exists
// -18 = Invalid cross-device link
// -19 = No such device
// -20 = Not a directory
// -21 = Not a file (?)
// -22 = Invalid argument
// -23 = Too many open files in system
// -24 = Too many open files
// -25 = Inappropriate ioctl for device
// -26 = Text file busy
// -27 = File too large
// -28 = No space left on device
// -29 = Illegal seek
// -30 = Read-only file system
// -31 = Too many links
// -32 = Broken pipe
// -33 = Numerical argument out of domain
// -34 = Numerical result out of range
// -35 = Resource deadlock avoided
// -36 = File name too long
// -37 = No locks available
// -38 = Function not implemented
// -39 = Directory not empty
// -40 = Too many levels of symbolic links
// -41 = Unknown error 41
// -42 = No message of desired type
// -43 = Identifier removed
// -44 = Channel number out of range
// -45 = Level 2 not synchronized
// -46 = Level 3 halted
// -47 = Level 3 reset
// -48 = Link number out of range
// -49 = Protocol driver not attached
// -50 = No CSI structure available
// -51 = Level 2 halted
// -52 = Invalid exchange
// -53 = Invalid request descriptor
// -54 = Exchange full
// -55 = No anode
// -56 = Invalid request code
// -57 = Invalid slot
// -58 = Unknown error 58
// -59 = Bad font file format
// -60 = Device not a stream
// -61 = No data available
// -62 = Timer expired
// -63 = Out of streams resources
// -64 = Machine is not on the network
// -65 = Package not installed
// -66 = Object is remote
// -67 = Link has been severed
// -68 = Advertise error
// -69 = Srmount error
// -70 = Communication error on send
// -71 = Protocol error
// -72 = Multihop attempted
// -73 = RFS specific error
// -74 = Bad message
// -75 = Value too large for defined data type
// -76 = Name not unique on network
// -77 = File descriptor in bad state
// -78 = Remote address changed
// -79 = Can not access a needed shared library
// -80 = Accessing a corrupted shared library
// -81 = .lib section in a.out corrupted
// -82 = Attempting to link in too many shared libraries
// -83 = Cannot exec a shared library directly
// -84 =  ... see: https://elixir.bootlin.com/u-boot/latest/source/include/linux/errno.h

/*

the goal is to provide one filesystem, over multiple instances
we have a json-file, that configures the directory settings
it is called: .dfs.json (distributed filesystem)

typically, the shadow fs on which the fuse is locally mounted, is the same directory as the fuse mount
the original files become only accessible through the fuse mount

 - when opening a directory, fetch a file list from all instances, and check which instance has the latest file version based on last modified date
 - make files/directories on remote instances available locally (download on open)
 - upon creation of a new file, the file must be uploaded so that it exists on at least 2 (=N) locations
 - if a file is updated or appended, also do this on the other locations it exists on
 - if a file is opened, it must be locked, so that other instances cannot simultaneously modify the file (but they may open it read-only)
   so the file becomes immutable, until it is closed
 - if another instance is not online, and it comes online, and a file was changed, and the local file is changed too, but the other file is later modification date
   then make a backup of the file, log this as a warning, and overwrite the file with the latest version as it comes online, the backup is named: ~filename.old (-1, -2, -...)

several plugins may be selected:
 - fuse: FUSE mount, where filesystem can be modified
 - cli: human-readable commands over command-line through terminal (list is \n separated, ""-quoting of special chars)
 - web: Website with filemanager interface
 - http: JSON API over HTTP
 - ws: JSON API over WebSocket

these plugins make it easy to use the filesystem even without locally mounting (so no need to install FUSE on local system to access the distributed filesystem)

we can also implement server on different languages
by default we only supply server-node.js, which is based on nodejs filesystem API for cross-platform usage

*/

function node_connect(node_address)
{
    return new Promise(function(resolve, reject)
    {
        var socket = net.connect({
            port: node_address.port,
            host: node_address.address
        });
        
        if(!socket) return reject(new Error('Could not connect to ' + node_address.address + ':' + node_address.port));
        
        var isConnected = false;
        
        socket.once('connect', function()
        {
            isConnected = true;
            resolve(socket);
        });
        socket.once('close', function()
        {
            if(isConnected) return;
            
            // cannot connect to server
            resolve(null);
        });
        
        var buf = '';
        socket.on('data', function(chunk)
        {
            buf += chunk;
            
            var n = buf.indexOf('\n');
            while(n !== -1)
            {
                try
                {
                    socket.emit('node_response', JSON.parse(buf.substring(0, n)));
                }
                catch(err) {}
                buf = buf.substring(n + 1);
                n = buf.indexOf('\n');
            }
        });
        socket.on('end', function()
        {
            if(buf.length > 0)
            {
                try
                {
                    socket.emit('node_response', JSON.parse(buf));
                }
                catch(err) {}
            }
        });
    });
}

function node_request(node, message)
{
    return new Promise(async function(resolve, reject)
    {
        // create connection if not yet exists
        if(!node.socket || node.socket.destroyed) node.socket = node_connect(node.address);
        
        // await for connection to be established (could also be created in parallel somewhere else, hence not directly awaiting above)
        var promise = null;
        if(typeof node.socket.then === 'function') promise = await node.socket;
        if(promise !== null && typeof node.socket.then === 'function')
        {
            // overwrite socket, but only once, if another listener is awaiting, then don't overwrite instance (although it's the same reference, so doesn't matter technically)
            node.socket = promise;
        }
        
        var request = {
            id: node.messageIdAutoIncr = (node.messageIdAutoIncr || 0) + 1,
            message: message
        };
        
        console.log('sending message: ' + JSON.stringify(message) + ' to ' + JSON.stringify(node.socket.address()));
        
        // write the message we wanted to send
        node.socket.write(JSON.stringify(request) + '\n');
        
        // wait for reply
        var reply_catcher = function(response)
        {
            if(typeof response !== 'object' || !response.id)
            {
                // socket closed or an error occurred
                
                node.socket.off('close', reply_catcher);
                node.socket.off('node_response', reply_catcher);
                
                return resolve(null);
            }
            
            if(response.id !== request.id) return;
            
            node.socket.off('close', reply_catcher);
            node.socket.off('node_response', reply_catcher);
            
            console.log('received message: ' + JSON.stringify(response.message) + ' from ' + JSON.stringify(node.socket.address()));
            
            resolve(response.message);
        };
        node.socket.on('node_response', reply_catcher);
        node.socket.on('close', reply_catcher);
    });
}

function fs_rmdir(path, verbose)
{
    return new Promise(function(resolve, reject)
    {
        fs.promises.rmdir(path).then(function()
        {
            resolve(true);
            
        }).catch(function(err)
        {
            if(verbose) console.error(err);
            
            resolve(false);
        });
    });
}

function fs_mkdir(path, verbose)
{
    return new Promise(function(resolve, reject)
    {
        fs.promises.mkdir(path).then(function()
        {
            resolve(true);
            
        }).catch(function(err)
        {
            if(verbose) console.error(err);
            
            resolve(false);
        });
    });
}

function fs_move(a, b, verbose)
{
    return new Promise(function(resolve, reject)
    {
        fs.promises.rename(a, b).then(function()
        {
            resolve(true);
            
        }).catch(function(err)
        {
            if(verbose) console.error(err);
            
            resolve(false);
        });
    });
}

function fs_exists(path, verbose)
{
    return new Promise(function(resolve, reject)
    {
        fs.promises.access(path).then(function()
        {
            resolve(true);
            
        }).catch(function(err)
        {
            if(verbose) console.error(err);
            
            resolve(false);
        });
    });
}

async function fs_create_shadow(shadowpath, mountpoint)
{
    // shadowpath may not exist yet
    if(await fs_exists(shadowpath))
    {
        console.error('error: Shadow path already exists, maybe another instance is already running on this mountpoint (' + shadowpath + ').');
        return false;
    }
    
    // move mountpoint to shadowpath
    if(!(await fs_move(mountpoint, shadowpath, true)))
    {
        console.error('error: Failed to move mountpoint to shadow path (' + mountpoint + ' => + ' + shadowpath + ').');
        return false;
    }
    
    // mountpoint must be created now
    if(!(await fs_mkdir(mountpoint, true)))
    {
        console.error('error: Failed to create mountpoint directory (' + mountpoint + ').');
        
        // if mountpoint directory already exists, something went wrong
        // or if we were unable to restore the shadowpath to mountpoint, this requires manual intervention
        if(await fs_exists(mountpoint) || !(await fs_move(shadowpath, mountpoint, true)))
        {
            console.error('error: The mountpoint has previously been moved to the shadow path (' + shadowpath + '). This must now be undone manually.');
        }
        return false;
    }
    
    return true;
}

async function fs_restore_shadow(shadow, mount)
{
    await fs_rmdir(mount, true);
    
    if(await fs_exists(mount))
    {
        console.error('error: Failed to restore mountpoint. Mountpoint exists and cannot be removed (' + mount + ').');
        return;
    }
    
    if(!(await fs_move(shadow, mount, true)))
    {
        console.error('error: Failed to restore mountpoint from shadow (' + shadow + ' => ' + mount + ').');
        return;
    }
}

async function parse_args(args)
{
    var arg_opts = {};
    
    for(var i=2;i<args.length;++i)
    {
        var arg = args[i];
        if(arg.startsWith('-'))
        {
            if(arg === '--')
            {
                break;
            }
            else if(arg === '-h' || arg === '--help')
            {
                console.log('Extensive usage: JDVFS_NODES=<nodes> ' + args[0] + ' --nodes <nodes> --config <file> --listen <address:port>[:/path/to/custom/shadow] --mount <mountpoint>');
                console.log('Basic usage: ' + args[0] + ' --listen :<port> --mount <mountpoint>');
                return;
            }
            // else if(arg === '--shadow-path')
            else if(arg === '--listen')
            {
                var val = args[++i];
                if(val.startsWith('--'))
                {
                    --i;
                    continue;
                }
                
                arg_opts.listen = await ConfigFile.parse_address(val);
            }
            else if(arg === '--mount')
            {
                arg_opts.mount = path.resolve(path.normalize(args[++i]));
            }
            else if(arg === '--config')
            {
                arg_opts.config = args[++i];
            }
            else if(arg === '--modules')
            {
                
            }
        }
    }
    
    return arg_opts;
}


async function main()
{
    console.log(process.argv);
    
    var arg_opts = await parse_args(process.argv);
    
    var shadow = false;
    var server = null;
    var config = null;
    var client = null;
    var cleaning = false;
    
    async function graceful_exit(auto)
    {
        if(!auto) // not auto means a signal was given or uncaught exception detected
        {
            // 2nd time exiting, force exit
            if(cleaning) return process.exit(1);
            
            cleaning = true;
        }
        
        // first close client
        if(client) await client.close();
        
        // then close server
        if(server) await server.close();
        
        // then restore directories if needed
        if(shadow) await fs_restore_shadow(arg_opts.listen.path, arg_opts.mount);
        
        // exit with code 0
        process.exit(0);
    }
    
    // when uncaught exception, log and exit
    
    process.on('uncaughtException', function(err)
    {
        if(err) console.error(err);
        graceful_exit();
    });
    
    // when signaling INT, we interrupt the process, and prepare for closing
    
    process.on('SIGINT', function()
    {
        console.log('info: Interrupt signal received. Stopping.');
        graceful_exit();
    });
    
    process.on('SIGTERM', function()
    {
        console.log('info: Termination signal received. Stopping.');
        graceful_exit();
    });
    
    
    // setup default path for listen based on mount if listen and mount:
    if(arg_opts.mount && arg_opts.listen && !arg_opts.listen.path)
    {
        // set default listen path to the shadow of the mount
        arg_opts.listen.path = path.join(path.dirname(arg_opts.mount), '.' + path.basename(arg_opts.mount));
        
        // create shadow mount
        if(!(await fs_create_shadow(arg_opts.listen.path, arg_opts.mount)))
        {
            console.error('error: Failed to create shadow. Exiting.');
            process.exit(1);
        }
        
        shadow = true;
    }
    
    
    console.log(arg_opts);
    
    
    // setup server:
    
    if(arg_opts.listen)
    {
        server = await ServerNode.create(arg_opts.listen);
        
        console.error('[server] started listening at ' + arg_opts.listen.address + ':' + arg_opts.listen.port + ':' + arg_opts.listen.path);
        
        server.on('error', function(err)
        {
            console.error('[server] error: ', err);
            
            graceful_exit(true);
        });
        server.on('close', function()
        {
            console.error('[server] stopped listening');
        });
    }
    
    
    // setup config:
    
    config = await ConfigFile.create({
        configFile: arg_opts.config,
        localNode: {address: arg_opts.listen, socket: null}
    });
    
    console.error(config._config);
    
    
    // setup client:
    
    client = await ClientFuse.create();
    
    // every time nodes changes, we must call update on client
    // client.update(config._config);
    
    // for some clients we have the .mount() option (with FUSE mount or other)
    // for other clients we have the .listen() option (with local socket listening)
    // and others we have .run() option (with stdin/stdout)
    client.mount(arg_opts.mount);
    
    client.on('mount', function()
    {
        console.error('[client] mounted at ' + arg_opts.mount);
    });
    client.on('error', function(err)
    {
        console.error('[client] error:', err);
        
        graceful_exit(true);
    });
    client.on('close', function()
    {
        console.error('[client] unmounted');
    });
    
    
    // dynamically load from config with require (and force updated from source files require on update)
    var mods = [
        ModFdcache.create(),    // keep cache of fd's, so that we may get fd from path, and still select the same node
        ModRedundancy.create()  // select the correct nodes to read/write from, with latest mtime (this should be a separate mod), and then add redundancy/backup mechanism
    ];
    
    // add merger modules:
    client.on('filesystem-event', function(key, args, cb)
    {
        var response = {
            STATUS_CODE: {
                SUCCESS: 0,
                NOT_PERMITTED: -1,
                NO_SUCH_FILE_OR_DIRECTORY: -2,
                FUNCTION_NOT_IMPLEMENTED: -38
            },
            statusCode: -38,
            result: null
        };
        
        var i = -1;
        var next = function(newStatus, newResult)
        {
            if(typeof newStatus !== 'undefined') response.statusCode = newStatus;
            if(typeof newResult !== 'undefined') response.result = newResult;
            
            if(i >= mods.length)
            {
                cb(response.status, response.result);
            }
            else
            {
                var mod = mods[i];
                
                // config is passed dynamically, as it may change dynamically
                mod.call({nodes: config._config.nodes, fs: client}, {action: key, arguments: args}, response, next);
            }
        };
        
        next();
    });
    
    
    // when signaling USR1, we reload configuration
    
    process.on('SIGUSR1', async function()
    {
        await config.reload();
        
        console.error('info: configuration updated:', config._config);
    });
}



main();
