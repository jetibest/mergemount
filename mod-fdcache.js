function flags_to_string(flags)
{
    // check if already converted to string
    if(typeof flags === 'string') return flags;
    
    const FCNTL = {
        O_RDONLY:    0x00000000,
        O_WRONLY:    0x00000001,
        O_RDWR:      0x00000002,
        O_CREAT:     0x00000100,
        O_EXCL:      0x00000200,
        O_NOCTTY:    0x00000400,
        O_TRUNC:     0x00001000,
        O_APPEND:    0x00002000,
        O_NONBLOCK:  0x00004000,
        O_DSYNC:     0x00010000,
        FASYNC:      0x00020000,
        O_DIRECT:    0x00040000,
        O_LARGEFILE: 0x00100000,
        O_DIRECTORY: 0x00200000,
        O_NOFOLLOW:  0x00400000,
        O_NOATIME:   0x01000000,
        O_CLOEXEC:   0x02000000,
        O_SYNC:      0x04000000,
        O_PATH:      0x01000000,
        O_TMPFILE:   0x02000000,
    };
    
    var values = [
        flags & 0xf,
        (flags & 0xf0) / 0x10,
        (flags & 0xf00) / 0x100,
        (flags & 0xf000) / 0x1000,
        (flags & 0xf0000) / 0x10000,
        (flags & 0xf00000) / 0x100000,
        (flags & 0xf000000) / 0x1000000,
        (flags & 0xf0000000) / 0x10000000,
    ];
    
    if(values[0] === 0) // O_RDONLY
    {
        return 'r';
    }
    else if(values[0] === 1) // O_WRONLY
    {
        if(values[3] === 2) // O_APPEND
        {
            if(values[2] === 1) // O_CREAT
            {
                return 'a';
            }
            return 'ax';
        }
        else if(values[2] === 1 || values[3] === 1) // O_CREAT || O_TRUNC
        {
            return 'w';
        }
        return 'wx';
    }
    else if(values[0] === 2) // O_RDWR
    {
        if(values[3] === 2) // O_APPEND
        {
            if(values[2] === 1) // O_CREAT
            {
                return 'a+';
            }
            return 'ax+';
        }
        else if(values[2] === 1 || values[3] === 1) // O_CREAT || O_TRUNC
        {
            return 'w+';
        }
        if(values[4] === 1 || values[4] === 2 || values[4] === 4) // O_DSYNC, FASYNC, O_DIRECT
        {
            return 'rs+';
        }
        return 'r+';
    }
}

function is_readable(flags)
{
    var v = flags & 0xf;
    return v === 0 || v === 2; // O_RDONLY || O_RDWR
}

function is_writable(flags)
{
    var v = flags & 0xf;
    return v === 1 || v === 2; // O_WRONLY || O_RDWR
}

// returns a node_handle
async function node_opendir(dir_path, flags)
{
    var handles = [];
    
    for(var i=0;i<this.nodes.length;++i)
    {
        var node = this.nodes[i];
        var handle = await node_request(node, {
            action: 'opendir',
            path: dir_path,
            flags: flags
        });
        
        if(handle !== null)
        {
            console.log('pushing handle: ', handle);
            
            handles.push({
                node: node,
                handle: handle // {fd, path}
            });
        }
    }
    
    return handles;
}

async function node_open(file_path, flags)
{
    // open on the latest node only
    // maybe after open check attr again, to see if this node still has highest mtime OR if any other node file is opened
    // then close it again straight away, because we got the wrong file
    // although maybe for reading mode, it's okay
    
    var latestAttr = await node_getattr.call(this, file_path);
    
    if(!latestAttr) return null;
    
    var handle = await node_request(latestAttr.node, {
        action: 'open',
        path: file_path,
        flags: flags
    });
    
    return {
        node: latestAttr.node,
        handle: handle // {fd, path}
    };
}

async function node_read(node_handle, buffer, length, position)
{
    var result = await node_request(node_handle.node, {
        action: 'read',
        fd: node_handle.handle.fd,
        length: length,
        position: position
    });
    
    if(!result || !result.buffer || result.buffer.type !== 'Buffer') return -1;
    
    var buf = Buffer.from(result.buffer.data, result.buffer.encoding || 'binary');
    buf.copy(buffer);
    return Math.min(buf.length, length);
}

async function node_write(node_handle, buffer, length, position)
{
    var result = await node_request(node_handle.node, {
        action: 'write',
        fd: node_handle.handle.fd,
        buffer: {type: 'Buffer', data: buffer.toString('base64'), encoding: 'base64'},
        length: length,
        position: position
    });
    
    if(!result || result.count < 0) return -1;
    
    return result.count;
}

async function node_truncate(node_handle, size)
{
    var result = await node_request(node_handle.node, {
        action: 'truncate',
        fd: node_handle.handle.fd,
        size: size
    });
    
    if(!result || result.error) return false;
    
    return result.result;
}

async function node_release(node_handles)
{
    if(!node_handles) return false;
    
    if(!Array.isArray(node_handles)) node_handles = [node_handles];
    
    for(var i=0;i<node_handles.length;++i)
    {
        var entry = node_handles[i];
        
        await node_request(entry.node, {
            action: 'release',
            fd: entry.handle.fd
        });
    }
    
    return node_handles.length > 0;
}

async function node_sync(node_handles)
{
    if(!node_handles) return false;
    
    if(!Array.isArray(node_handles)) node_handles = [node_handles];
    
    for(var i=0;i<node_handles.length;++i)
    {
        var entry = node_handles[i];
        
        await node_request(entry.node, {
            action: 'sync',
            fd: entry.handle.fd
        });
    }
    
    return node_handles.length > 0;
}

async function node_utimens(path, atime, mtime)
{
    var lastAttr = await node_getattr.call(this, path);
    
    // does not exist
    if(!lastAttr) return false;
    
    var result = await node_request(lastAttr.node, {
        action: 'utimens',
        path: path,
        atime: atime,
        mtime: mtime
    });
    
    console.log('utimens result:', result);
    
    if(!result || result.error) return false;
    
    return true;
}

async function node_unlink(path)
{
    for(var i=0;i<this.nodes.length;++i)
    {
        // check error, if error was not no such file or directory, then we must return the error (although at other nodes it is free to succeed)
        var result = await node_request(this.nodes[i], {
            action: 'unlink',
            path: path
        });
        
        console.log('unlink result:', result);
    }
    
    return true;
}

async function node_rename(src, dst)
{
    for(var i=0;i<this.nodes.length;++i)
    {
        // if one error occurred, and not no such file or directory, then immediately abort
        var result = await node_request(this.nodes[i], {
            action: 'rename',
            oldpath: src,
            newpath: dst
        });
        
        console.log('rename result:', result);
    }
    
    return true;
}

async function node_symlink(target, linkpath)
{
    // file_mode = file_mode || 33188;
    var dir_path = path.dirname(linkpath);
    
    // check on which node the parent path exists
    // if none, then fail to create
    var lastAttr = await node_getattr.call(this, dir_path);
    
    // parent directory of file_path does not exist on any node
    if(lastAttr === null) return false;
    
    var result = await node_request(lastAttr.node, {
        action: 'symlink',
        target: target,
        linkpath: linkpath
    });
    
    console.log('symlink result:', result);
    
    if(!result || result.error) return false;
    
    return true;
}

async function node_mkdir(dirpath, mode)
{
    // file_mode = file_mode || 33188;
    var dir_path = path.dirname(dirpath);
    
    // check on which node the parent path exists
    // if none, then fail to create
    var lastAttr = await node_getattr.call(this, dir_path);
    
    // parent directory of file_path does not exist on any node
    if(lastAttr === null) return false;
    
    var result = await node_request(lastAttr.node, {
        action: 'mkdir',
        path: dirpath,
        mode: mode
    });
    
    console.log('mkdir result:', result);
    
    if(!result || result.error) return false;
    
    return true;
}

async function node_rmdir(path)
{
    for(var i=0;i<this.nodes.length;++i)
    {
        var result = await node_request(this.nodes[i], {
            action: 'rmdir',
            path: path
        });
        
        console.log('rmdir result:', result);
    }
    
    return true;
}

async function node_readlink(path)
{
    var lastAttr = await node_getattr.call(this, path);
    
    if(lastAttr === null) return false;
    
    var result = await node_request(lastAttr.node, {
        action: 'readlink',
        path: path
    });
    
    if(!result || result.error) return false;
    
    return result.path;
}

async function node_access(path, mode)
{
    var lastAttr = await node_getattr.call(this, path);
    
    if(lastAttr === null) return false;
    
    var result = await node_request(lastAttr.node, {
        action: 'access',
        path: path,
        mode: mode
    });
    
    if(!result || result.error) return false;
    
    // true or false, depending on whether access with mode succeeded or not
    return result.result;
}

// returns [] from node_handle
async function node_readdir(node_handles)
{
    var list = [];
    
    for(var i=0;i<node_handles.length;++i)
    {
        var entry = node_handles[i];
        var items = await node_request(entry.node, {
            action: 'readdir',
            path: entry.handle.path
        });
        if(items)
        {
            for(var j=0;j<items.length;++j)
            {
                if(list.indexOf(items[j]) === -1)
                {
                    list.push(items[j]);
                }
            }
        }
    }
    
    return list;
}

// closes a node_handle
async function node_closedir(node_handles)
{
    for(var i=0;i<node_handles.length;++i)
    {
        var entry = node_handles[i];
        
        await node_request(entry.node, {
            action: 'closedir',
            fd: entry.handle.fd
        });
    }
}

// create a file
async function node_create(file_path, file_mode)
{
    // file_mode = file_mode || 33188;
    var dir_path = path.dirname(file_path);
    
    // check on which node the parent path exists
    // if none, then fail to create
    var lastAttr = await node_getattr.call(this, dir_path);
    
    // parent directory of file_path does not exist on any node
    if(lastAttr === null) return false;
    
    var result = await node_request(lastAttr.node, {
        action: 'create',
        path: file_path,
        mode: file_mode
    });
    
    if(!result || result.error) return false;
    
    return true;
}

// get attributes of a path
async function node_getattr(path)
{
    // grab attributes of every node, and return the one that has the latest mtime
    var lastAttr = null;
    var lastAttrNode = null;
    var t0 = Date.now();
    
    for(var i=0;i<this.nodes.length;++i)
    {
        var attr = await node_request(this.nodes[i], {
            action: 'getattr',
            path: path
        });
        
        if(attr)
        {
            if(attr.stat)
            {
                if(lastAttr === null || attr.stat.mtime > lastAttr.mtime)
                {
                    lastAttr = attr.stat;
                    lastAttrNode = this.nodes[i];
                }
            }
            else
            {
                // handle attr.error
            }
        }
        else
        {
            // handle node_request error
        }
    }
    
    // path does not exist
    if(lastAttr === null)
    {
        return null;
    }
    
    // convert epoch_ms to date objects:
    lastAttr.mtime = new Date(lastAttr.mtime || 0);
    lastAttr.atime = new Date(lastAttr.atime || 0);
    lastAttr.ctime = new Date(lastAttr.ctime || 0);
    
    return {
        node: lastAttrNode,
        attr: lastAttr,
        lastUpdateStartEpochMS: Date.now(),
        lastUpdateEndEpochMS: Date.now()
        // the real time at which we checked the attr is between start and end, we cannot be sure about the exact timing
    };
    /*
    return {
        mtime: new Date(),
        atime: new Date(),
        ctime: new Date(),
        size: 100,
        mode: 16877,
        uid: process.getuid(),
        gid: process.getgid()
    };*/
}


module.exports = {
    create: function()
    {
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
        
        return async function(req, res, next)
        {
            // this.nodes
            
            if(res.statusCode === res.STATUS_CODE.SUCCESS) return next();
            
            if(req.action === 'init')
            {
                next(0);
            }
            else if(req.action === 'fgetattr')
            {
                this.source.emit('filesystem-event', 'fgetattr', {path: req.arguments.path}, next);
            }
            else if(req.action === 'flush')
            {
                this.source.emit('filesystem-event', 'fsync', {path: req.arguments.path, fd: req.arguments.fd, datasync: 0}, next);
            }
            else if(req.action === 'fsyncdir')
            {
                this.source.emit('filesystem-event', 'fsync', {path: req.arguments.path, fd: req.arguments.fd, datasync: req.arguments.datasync}, next);
            }
            else if(req.action === 'access')
            {
                if(await node_access.call(this, req.arguments.path, req.arguments.mode))
                {
                    next(0);
                }
                else
                {
                    next(-2);
                }
            }
            else if(req.action === 'statfs')
            {
                // for req.arguments.path
                
                next(0, {
                    bsize: 1000000,
                    frsize: 1000000,
                    blocks: 1000000,
                    bfree: 1000000,
                    bavail: 1000000,
                    files: 1000000,
                    ffree: 1000000,
                    favail: 1000000,
                    fsid: 1000000,
                    flag: 1000000,
                    namemax: 1000000
                });
            }
            else if(req.action === 'getattr')
            {
                var attr_result = await node_getattr.call(this, req.arguments.path);
                
                if(attr_result)
                {
                    return next(0, attr_result.attr);
                }
                
                next(-2);
            }
            else if(req.action === 'fsync')
            {
                // FUSE docs: If the datasync parameter is non-zero, then only the user data should be flushed, not the meta data. 
                
                var fd = req.arguments.fd;
                
                if(fd === 0)
                {
                    // grab fd from path:
                    fd = fd_store.node_handles_by_path[req.arguments.path];
                }
                
                var fh = fd_store.node_handles[fd];
                
                // check if exists
                if(!fh)
                {
                    // then try to open by path
                    fd = await new Promise(resolve => this.fs.emit('filesystem-event', 'open', {path: req.arguments.path, flags: 'w'}, resolve)));
                    fh = fd_store.node_handles[fd];
                    
                    if(!fh)
                    {
                        console.log('fsync failed without fh, for: ', req.arguments.path, fd);
                        return next(-1);
                    }
                }
                
                // if readable only, then there is nothing to do, buffers are automatically freed/cleared/flushed
                if(!is_writable(fh.flags)) return next(0);
                
                if(await node_sync.call(this, fh.handles, datasync))
                {
                    next(datasync);
                }
                else
                {
                    console.log('fsync failed for:', fh);
                    next(-1);
                }
            }
            else if(req.action === 'readdir')
            {
                var path = req.arguments.path;
                
                var fd = fd_store.node_handles_by_path[path];
                
                // if(!fd) --> try opendir on the fly?
                
                if(fd)
                {
                    var fd_item = fd_store.node_handles[fd];
                    
                    // check if exists, and sanity checking
                    if(fd_item && fd_item.path === path)
                    {
                        var items = await node_readdir.call(this, fd_item.handles);
                        
                        if(items !== null)
                        {
                            return next(0, items);
                        }
                    }
                }
                
                next(-1);
            }
            else if(req.action === 'truncate')
            {
                var fd = fd_store.node_handles_by_path[path];
                
                if(!fd) return next(-1);
                
                this.source.emit('filesystem-event', 'ftruncate', {path: req.arguments.path, fd: req.arguments.fd, size: req.arguments.size}, next);
            }
            else if(req.action === 'ftruncate')
            {
                var fd = req.arguments.fd;
                var size = req.arguments.size;
                
                var fh = fd_store.node_handles[fd];
                
                // check if exists
                if(!fh) return next(-1);
                
                // check if flags is correct, has to contain 'w'
                if(!is_writable(fh.flags)) return next(-1);
                
                // only one handle should exist for this fd
                var handle = fh.handles[0];
                
                // sanity checking
                if(!handle) return next(-1);
                
                if(await node_truncate.call(this, handle, size))
                {
                    next(0);
                }
                else
                {
                    next(-1);
                }
            }
            else if(req.action === 'readlink')
            {
                var result = await node_readlink.call(this, req.arguments.path);
                
                if(!result) return next(-1);
                
                next(0, result);
            }
            else if(req.action === 'opendir')
            {
                // open directory for reading (local or remote)
                var handles = await node_opendir.call(this, req.arguments.path, req.arguments.flags);
                
                // maybe directory does not exist, or no permissions
                if(handles === null || handles.length === 0) return next(res.STATUS_CODE.NO_SUCH_FILE_OR_DIRECTORY);
                
                var fd = fd_store.next_fd();
                
                fd_store.node_handles[fd] = {
                    type: 'd',
                    path: path,
                    flags: flags,
                    handles: handles
                };
                fd_store.node_handles_by_path[path] = fd; // but this could also be an array with multiple fd
                
                next(res.STATUS_CODE.SUCCESS, fd);
            }
            else if(req.action === 'chown')
            {
                // path, uid, gid
                
                next(-1);
            }
            else if(req.action === 'getxattr')
            {
                next(0, null); // don't fail, but just don't return any
            }
            else if(req.action === 'open')
            {
                var path = req.arguments.path;
                var flags = req.arguments.flags;
                
                // we could use cached: var latestAttr = latest_attr[path];, for selecting the node to open at
                
                // open file for reading (local or remote)
                var handle = await node_open.call(this, path, flags);
                
                // maybe file does not exist, or no permissions
                if(!handle) return next(-1);
                
                var fd = fd_store.next_fd();
                
                fd_store.node_handles[fd] = {
                    type: 'f',
                    path: path,
                    flags: flags,
                    handles: [handle]
                };
                fd_store.node_handles_by_path[path] = fd;
                
                return next(0, fd);
            }
            else if(req.action === 'read')
            {
                var fd = req.arguments.fd;
                
                var fh = fd_store.node_handles[fd];
                
                // check if exists
                if(!fh) return next(res.STATUS_CODE.NO_SUCH_FILE_OR_DIRECTORY);
                
                // check if flags is correct, has to contain 'r'
                if(!is_readable(fh.flags)) return next(-1);
                
                // only one handle should exist for this fd
                var handle = fh.handles[0];
                
                // sanity checking
                if(!handle) return next(-1);
                
                var bytes_read = await node_read.call(this, handle, req.arguments.buffer, req.arguments.length, req.arguments.position);
                
                if(bytes_read < 0) return next(-1);
                
                next(bytes_read);
            }
            else if(req.action === 'write')
            {
                var fh = fd_store.node_handles[fd];
                
                // check if exists
                if(!fh) return next(res.STATUS_CODE.NO_SUCH_FILE_OR_DIRECTORY);
                
                // check if flags is correct, has to contain 'w'
                if(!is_writable(fh.flags)) return next(-1);
                
                // only one handle should exist for this fd
                var handle = fh.handles[0];
                
                // sanity checking
                if(!handle) return next(-1);
                
                var bytes_written = await node_write.call(this, handle, req.arguments.buffer, req.arguments.length, req.arguments.position);
                
                if(bytes_written < 0) return next(-1);
                
                next(bytes_written);
            }
            else if(action === 'release')
            {
                var fd = req.arguments.fd;
                
                var fh = fd_store.node_handles[fd];
                
                // check if exists
                if(!fh) return next(-1);
                
                if(await node_release(fh.handles))
                {
                    delete fd_store.node_handles_by_path[fh.path];
                    delete fd_store.node_handles[fd];
                    
                    next(0);
                }
                else
                {
                    next(-1);
                }
            }
            else if(action === 'releasedir')
            {
                var fd = req.arguments.fd;
                var path = req.arguments.path;
                
                var fd_item = fd_store.node_handles[fd];
                
                if(!fd_item)
                {
                    // maybe release by path instead of by fd
                    fd = fd_store.node_handles_by_path[path];
                    if(fd)
                    {
                        fd_item = fd_store.node_handles[fd];
                    }
                }
                
                if(fd_item)
                {
                    // do additional cleanup on this filedescriptor (at backend)
                    await node_closedir.call(this, fd_item.handles);
                    
                    delete fd_store.node_handles_by_path[fd_item.path];
                    delete fd_store.node_handles[fd];
                    
                    return next(0);
                }
                
                next(-1);
            }
            else if(action === 'create')
            {
                var path = req.arguments.path;
                var mode = req.arguments.mode;
                
                // create file with the given mode (chmod)
                if(await node_create.call(this, path, mode))
                {
                    next(0);
                }
                else
                {
                    next(-1);
                }
            }
            else if(action === 'utimens')
            {
                var path = req.arguments.path;
                var atime = req.arguments.atime;
                var mtime = req.arguments.mtime;
                
                // update atime and/or mtime
                if(await node_utimens.call(this, path, atime, mtime))
                {
                    next(0);
                }
                else
                {
                    next(-1);
                }
            }
            else if(action === 'unlink')
            {
                // delete file
                if(await node_unlink.call(this, path))
                {
                    next(0);
                }
                else
                {
                    next(-1);
                }
            }
            else if(action === 'rename')
            {
                var oldpath = req.arguments.oldpath;
                var newpath = req.arguments.newpath;
                
                // move file
                if(await node_rename.call(this, oldpath, newpath))
                {
                    next(0);
                }
                else
                {
                    next(-1);
                }
            }
            else if(action === 'symlink')
            {
                var target = req.arguments.target;
                var linkpath = req.arguments.linkpath;
                
                if(await node_symlink.call(this, target, linkpath))
                {
                    next(0);
                }
                else
                {
                    next(-1);
                }
            }
            else if(action === 'mkdir')
            {
                var path = req.arguments.path;
                var mode = req.arguments.mode;
                
                if(await node_mkdir.call(this, path, mode))
                {
                    next(0);
                }
                else
                {
                    next(-1);
                }
            }
            else if(action === 'rmdir')
            {
                var path = req.arguments.path;
                
                if(await node_rmdir.call(this, path))
                {
                    next(0);
                }
                else
                {
                    next(-1);
                }
            }
        };
    }
};
