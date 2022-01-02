const net = require('net');
const path = require('path');
const events = require('events');
const Fuse = require('fuse-native');

function configure_fuse(dontTryAgain)
{
    return new Promise(function(resolve, reject)
    {
        Fuse.isConfigured(function(isConfigured)
        {
            if(isConfigured)
            {
                resolve(true);
            }
            else if(!dontTryAgain)
            {
                Fuse.configure(async function()
                {
                    await configure_fuse(true).then(resolve).catch(reject);
                });
            }
            else
            {
                reject(new Error('warning: FUSE may not be configured. Do this manually (with admin/root privileges) if FUSE refuses to mount.'));
            }
        });
    });
}

function init_handlers()
{
    const self = this;
    
    var fd_store = {
        next_fd: function()
        {
            var i = 0;
            while(this.node_handles[++i]) {}
            return i;
        },
        node_handles: {},
        node_handles_by_path: {} // lookup for getting fd based on path (since readdir supplies path instead of fd)
    };
    var latest_attr = {};
    
    const fuse_handlers = {
        init: function(cb)
        {
            console.log('init()');
            
            // on filesystem init
            self.emit('filesystem-event', 'init', {}, cb);
        },
        access: async function(path, mode, cb)
        {
            console.log('access(): ' + path +  ', ' + mode);
            
            self.emit('filesystem-event', 'access', {path: path, mode: mode}, cb);
        },
        statfs: function(path, cb)
        {
            console.log('statfs(): ' + path);
            
            self.emit('filesystem-event', 'statfs', {path: path}, cb);
        },
        getattr: async function(path, cb)
        {
            console.log('getattr(): ' + path);
            
            self.emit('filesystem-event', 'getattr', {path: path}, cb);
        },
        fgetattr: async function(path, fd, cb)
        {
            console.log('fgetattr(): ' + path +  ', ' + fd + ' --> forwarding to getattr(path, cb)');
            
            self.emit('filesystem-event', 'fgetattr', {path: path, fd: fd}, cb);
        },
        flush: function(path, fd, cb)
        {
            console.log('flush(): ' + path + ', ' + fd);
            
            self.emit('filesystem-event', 'flush', {path: path, fd: fd}, cb);
        },
        fsync: async function(path, fd, datasync, cb)
        {
            console.log('fsync(): ' + path + ', ' + fd + ', ' + datasync);
            
            self.emit('filesystem-event', 'fsync', {path: path, fd: fd, datasync: datasync}, cb);
        },
        fsyncdir: function(path, fd, datasync, cb)
        {
            console.log('fsyncdir(): ' + path + ', ' + fd + ', ' + datasync);
            
            self.emit('filesystem-event', 'fsyncdir', {path: path, fd: fd, datasync: datasync}, cb);
        },
        readdir: async function(path, cb)
        {
            console.log('readdir(): ' + path);
            
            self.emit('filesystem-event', 'readdir', {path: path}, cb);
        },
        truncate: async function(path, size, cb)
        {
            console.log('truncate(): ' + path + ', ' + size);
            
            self.emit('filesystem-event', 'truncate', {path: path, size: size}, cb);
        },
        ftruncate: async function(path, fd, size, cb)
        {
            console.log('ftruncate(): ' + path + ', ' + fd + ', ' + size);
            
            self.emit('filesystem-event', 'ftruncate', {path: path, fd: fd, size: size}, cb);
        },
        readlink: async function(path, cb)
        {
            console.log('readlink(): ' + path);
            
            self.emit('filesystem-event', 'readlink', {path: path}, cb);
        },
        chown: function(path, uid, gid, cb)
        {
            console.log('chown(): ' + path + ', ' + uid + ', ' + gid);
            
            self.emit('filesystem-event', 'chown', {path: path, uid: uid, gid: gid}, cb);
        },
        chmod: function(path, mode, cb)
        {
            console.log('chmod(): ' + path + ', ' + mode);
            
            self.emit('filesystem-event', 'chmod', {path: path, mode: mode}, cb);
        },
        mknod: function(path, mode, dev, cb)
        {
            console.log('mknod(): ' + path + ', ' + mode + ', ' + dev);
            
            self.emit('filesystem-event', 'mknod', {path: path, mode: mode, dev: dev}, cb);
        },
        setxattr: function(path, name, value, position, flags, cb)
        {
            console.log('setxattr(): ' + path + ', ' + name + ', ' + value + ', ' + position + ', ' + flags);
            
            self.emit('filesystem-event', 'setxattr', {path: path, name: name, value: value, position: position, flags: flags}, cb);
        },
        getxattr: function(path, name, position, cb)
        {
            console.log('getxattr(): ' + path + ', ' + name + ', ' + position);
            
            // xattr is not supported, but do not return an error, instead return as if no xattr exists
            self.emit('filesystem-event', 'getxattr', {path: path, name: name, position: position}, cb);
        },
        listxattr: function(path, cb)
        {
            console.log('listxattr(): ' + path);
            
            self.emit('filesystem-event', 'listxattr', {path: path}, cb);
        },
        removexattr: function(path, name, cb)
        {
            console.log('remotexattr(): ' + path + ', ' + name);
            
            self.emit('filesystem-event', 'removexattr', {path: path, name: name}, cb);
        },
        open: async function(path, flags, cb)
        {
            console.log('open(): ' + path + ', ' + flags);
            
            self.emit('filesystem-event', 'open', {path: path, flags: flags}, cb);
        },
        opendir: async function(path, flags, cb)
        {
            console.log('opendir(): ' + path + ', ' + flags);
            
            self.emit('filesystem-event', 'opendir', {path: path, flags: flags}, cb);
        },
        read: async function(path, fd, buffer, length, position, cb)
        {
            console.log('read(): ' + path + ', ' + fd + ', ' + buffer + ', ' + length + ', ' + position);
            
            self.emit('filesystem-event', 'read', {path: path, fd: fd, buffer: buffer, length: length, position: position}, cb);
        },
        write: async function(path, fd, buffer, length, position, cb)
        {
            console.log('write(): ' + path + ', ' + fd + ', ' + buffer + ', ' + length + ', ' + position);
            
            self.emit('filesystem-event', 'write', {path: path, fd: fd, buffer: buffer, length: length, position: position}, cb);
        },
        release: async function(path, fd, cb)
        {
            console.log('release(): ' + path + ', ' + fd);
            
            self.emit('filesystem-event', 'release', {path: path, fd: fd}, cb);
        },
        releasedir: async function(path, fd, cb)
        {
            console.log('releasedir(): ' + path + ', ' + fd);
            
            self.emit('filesystem-event', 'releasedir', {path: path, fd: fd}, cb);
        },
        create: async function(path, mode, cb)
        {
            console.log('create(): ' + path + ', ' + mode);
            
            self.emit('filesystem-event', 'create', {path: path, mode: mode}, cb);
        },
        utimens: async function(path, atime, mtime, cb)
        {
            console.log('utimens(): ' + path + ', ' + atime + ', ' + mtime);
            
            self.emit('filesystem-event', 'utimens', {path: path, atime: atime, mtime: mtime}, cb);
        },
        unlink: async function(path, cb)
        {
            console.log('unlink(): ' + path);
            
            self.emit('filesystem-event', 'unlink', {path: path}, cb);
        },
        rename: async function(src, dest, cb)
        {
            console.log('rename(): ' + src + ', ' + dest);
            
            self.emit('filesystem-event', 'rename', {oldpath: src, newpath: dest}, cb);
        },
        link: async function(src, dest, cb)
        {
            console.log('link(): ' + src + ', ' + dest);
            
            self.emit('filesystem-event', 'link', {oldpath: src, newpath: dest}, cb);
        },
        symlink: async function(src, dest, cb)
        {
            console.log('symlink(): ' + src + ', ' + dest);
            
            self.emit('filesystem-event', 'symlink', {target: src, linkpath: dest}, cb);
        },
        mkdir: async function(path, mode, cb)
        {
            console.log('mkdir(): ' + path + ', ' + mode);
            
            self.emit('filesystem-event', 'mkdir', {path: path, mode: mode}, cb);
        },
        rmdir: async function(path, cb)
        {
            console.log('rmdir(): ' + path);
            
            self.emit('filesystem-event', 'rmdir', {path: path}, cb);
        }
    };
    
    return fuse_handlers;
}

module.exports = {
    install: function()
    {
        return configure_fuse();
    },
    create: async function()
    {
        const self = new events();
        
        await configure_fuse().catch(console.error); // ignore warning, try to continue without configuring FUSE for installations which already have FUSE installed
        
        self._config = {nodes: []};
        self._isClosed = false;
        self._isMounted = false;
        
        self.update = function update(config)
        {
            self._config = config;
        };
        
        self.mount = function mount(path, options)
        {
            path = path || '.';
            options = options || {};
            
            // careful, when allowOther or allowRoot, we must set user_allow_other in /etc/fuse.conf
            
            self._fuse = new Fuse(path, init_handlers.call(self), {});
            
            self._fuse.mount(function(err)
            {
                if(err)
                {
                    console.error(err);
                    self.emit('error', err);
                    return self.close();
                }
                
                console.error('info: FUSE mounted at ' + path);
                
                self._isMounted = true;
                self.emit('ready');
            });
        };
        
        self.close = function close()
        {
            self._isClosed = true;
            self.emit('close');
            
            return new Promise(function(resolve, reject)
            {
                self._fuse.unmount(function(err)
                {
                    resolve(err);
                });
            });
        };
        
        return self;
    }
};
