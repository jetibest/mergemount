const fs = require('fs');
const net = require('net');
const path = require('path');
const events = require('events');

// TODO: improved error handling, and passing through errors through uniform result objects

function local_opendir(dir_path, flags)
{
    const self = this;
    
    var apath = path.join(this._rootPath, '.' + path.sep + dir_path);
    
    return new Promise(function(resolve, reject)
    {
        // flags are not used currently
        fs.promises.opendir(apath, {encoding: 'utf8', bufferSize: 32}).then(function(dir_handle)
        {
            var fd = self._next_fd();
            self._handles[fd] = {
                type: 'd',
                handle: dir_handle
            };
            self._handles_lookup_path[dir_path] = fd;
            
            resolve({fd: fd, path: dir_path});
            
        }).catch(function(err)
        {
            console.error(err);
            
            resolve(null);
        });
    });
}

async function local_readdir(dir_path)
{
    var fd = this._handles_lookup_path[dir_path];
    
    if(!fd) return null;
    
    var handle = this._handles[fd];
    
    if(!handle || handle.type !== 'd') return null;
    
    var list = [];
    
    var entry;
    while((entry = await handle.handle.read()) !== null)
    {
        list.push(entry.name);
    }
    
    return list;
}

async function local_getattr(file_path)
{
    var apath = path.join(this._rootPath, '.' + path.sep + file_path);
    
    return new Promise(function(resolve, reject)
    {
        fs.promises.lstat(apath).then(function(stat)
        {
            resolve({
                stat: {
                    mtime: Math.floor(stat.mtimeMs),
                    atime: Math.floor(stat.atimeMs),
                    ctime: Math.floor(stat.ctimeMs),
                    size: stat.size,
                    mode: stat.mode,
                    uid: stat.uid,
                    gid: stat.gid
                }
            });
            
        }).catch(function(err)
        {
            console.error(err);
            resolve({
                error: err
            });
        });
    });
}

async function local_closedir(fd)
{
    var handle = this._handles[fd];
    
    if(!handle || handle.type !== 'd') return false;
    
    await handle.handle.close();
    
    return true;
}

async function local_open(file_path, flags)
{
    const self = this;
    
    var apath = path.join(this._rootPath, '.' + path.sep + file_path);
    
    return new Promise(function(resolve, reject)
    {
        fs.promises.open(apath, flags) // , permissions => 0o666
        .then(function(file_handle)
        {
            var fd = self._next_fd();
            self._handles[fd] = {
                type: 'f',
                handle: file_handle
            };
            self._handles_lookup_path[file_path] = fd;
            
            resolve({
                fd: fd,
                path: file_path
            });
        })
        .catch(function(err)
        {
            console.error(err);
            resolve(null);
        });
    });
    
    return {
        fd: 0,
        path: file_path
    };
}

async function local_read(fd, len, pos)
{
    var fh = this._handles[fd];
    
    if(!fh || fh.type !== 'f') return null;
    
    var buf = Buffer.allocUnsafe(len);
    
    return new Promise(function(resolve, reject)
    {
        fh.handle.read({
            buffer: buf,
            offset: 0,
            length: len,
            position: pos || null
        }).then(function(result)
        {
            resolve({
                buffer: {type: 'Buffer', data: buf.slice(0, result.bytesRead).toString('base64'), encoding: 'base64'}
            });
            
        }).catch(function(err)
        {
            console.error(err);
            resolve({
                buffer: null,
                error: err
            });
        });
    });
}

async function local_write(fd, buf, len, pos)
{
    var fh = this._handles[fd];
    
    if(!fh || fh.type !== 'f') return null;
    
    return new Promise(function(resolve, reject)
    {
        // {type: 'Buffer', data: [] or '', encoding: 'optional-encoding-defaults-to-binary'}
        buf = Buffer.from(buf.data, buf.encoding || 'binary');
        
        fh.handle.write(
            buf,
            0,
            len || buf.byteLength,
            pos || 0
        ).then(function(result)
        {
            resolve({
                count: result.bytesWritten
            });
            
        }).catch(function(err)
        {
            console.error(err);
            resolve({
                count: -1,
                error: err
            });
        });
    });
}

async function local_truncate(fd, size)
{
    var fh = this._handles[fd];
    
    if(!fh || fh.type !== 'f') return false;
    
    return new Promise(function(resolve, reject)
    {
        fh.handle.truncate(size || 0).then(function()
        {
            resolve({
                result: true
            });
            
        }).catch(function(err)
        {
            resolve({
                error: err
            });
        });
    });
}

async function local_release(fd)
{
    var fh = this._handles[fd];
    
    if(!fh || fh.type !== 'f') return null;
    
    return fh.handle.close();
}

async function local_sync(fd)
{
    var fh = this._handles[fd];
    
    if(!fh) return null;
    
    if(typeof fh.datasync === 'function')
    {
        return fh.datasync();
    }
    
    // nothing to do for directory handles
    return true;
}

async function local_create(file_path, mode)
{
    var apath = path.join(this._rootPath, '.' + path.sep + file_path);
    
    return new Promise(function(resolve, reject)
    {
        fs.promises.open(apath, 'w', mode).then(async function(fh)
        {
            await fh.close();
            
            resolve(true);
            
        }).catch(function(err)
        {
            console.error(err);
            resolve(null);
        });
    });
}

async function local_utimens(rpath, atime, mtime)
{
    var apath = path.join(this._rootPath, '.' + path.sep + rpath);
    
    return new Promise(function(resolve, reject)
    {
        fs.promises.utimes(apath, atime, mtime).then(function(result)
        {
            resolve({result: true});
            
        }).catch(function(err)
        {
            console.error(err);
            resolve({error: err});
        });
    });
}

async function local_unlink(rpath)
{
    var apath = path.join(this._rootPath, '.' + path.sep + rpath);
    
    return new Promise(function(resolve, reject)
    {
        fs.promises.unlink(apath).then(function(result)
        {
            resolve({result: true});
            
        }).catch(function(err)
        {
            console.error(err);
            resolve({error: err});
        });
    });
}

async function local_rename(oldpath, newpath)
{
    var absolute_oldpath = path.join(this._rootPath, '.' + path.sep + oldpath);
    var absolute_newpath = path.join(this._rootPath, '.' + path.sep + newpath);
    
    return new Promise(function(resolve, reject)
    {
        fs.promises.rename(absolute_oldpath, absolute_newpath).then(function(result)
        {
            resolve({result: true});
            
        }).catch(function(err)
        {
            console.error(err);
            resolve({error: err});
        });
    });
}

async function local_symlink(target, linkpath)
{
    var actual_target = path.isAbsolute(target) ? path.join(this._rootPath, '.' + path.sep + target) : target;
    var absolute_linkpath = path.join(this._rootPath, '.' + path.sep + linkpath);
    
    return new Promise(function(resolve, reject)
    {
        fs.promises.symlink(actual_target, absolute_linkpath).then(function(result)
        {
            resolve({result: true});
            
        }).catch(function(err)
        {
            // maybe retry with type 'dir' on Windows (in third argument of fs.promises.symlink()), which defaults to 'file'
            
            console.error(err);
            resolve({error: err});
        });
    });
}

async function local_mkdir(rpath, mode)
{
    var apath = path.join(this._rootPath, '.' + path.sep + rpath);
    
    return new Promise(function(resolve, reject)
    {
        fs.promises.mkdir(apath, {mode: mode}).then(function(result)
        {
            resolve({result: true});
            
        }).catch(function(err)
        {
            console.error(err);
            resolve({error: err});
        });
    });
}

async function local_rmdir(rpath)
{
    var apath = path.join(this._rootPath, '.' + path.sep + rpath);
    
    return new Promise(function(resolve, reject)
    {
        fs.promises.rmdir(apath).then(function(result)
        {
            resolve({result: true});
            
        }).catch(function(err)
        {
            console.error(err);
            resolve({error: err});
        });
    });
}

async function local_readlink(rpath)
{
    var apath = path.join(this._rootPath, '.' + path.sep + rpath);
    
    return new Promise(function(resolve, reject)
    {
        fs.promises.readlink(apath, {encoding: 'utf8'}).then(function(result)
        {
            resolve({path: result});
            
        }).catch(function(err)
        {
            console.error(err);
            resolve({error: err});
        });
    });
}

async function local_access(rpath, mode)
{
    var apath = path.join(this._rootPath, '.' + path.sep + rpath);
    
    return new Promise(function(resolve, reject)
    {
        fs.promises.access(apath, mode).then(function(result)
        {
            resolve({result: true});
            
        }).catch(function(err)
        {
            console.error(err);
            resolve({error: err});
        });
    });
}


function local_socket(socket)
{
    const self = this;
    
    // first read the target command in JSON, and then translate to a local function, then return the result in JSON
    // one JSON per newline
    
    var buf = '';
    socket.on('data', function(chunk)
    {
        buf += chunk;
        
        var n = buf.indexOf('\n');
        while(n !== -1)
        {
            try
            {
                socket.emit('node_request', JSON.parse(buf.substring(0, n)));
            }
            catch(err) {}
            buf = buf.substring(n + 1);
            n = buf.indexOf('\n');
        }
    });
    
    socket.on('node_request', async function(request)
    {
        var request_message = request.message;
        var response_id = request.id;
        var response_message = null;
        
        if(request_message.action === 'opendir')
        {
            response_message = await local_opendir.call(self, request_message.path, request_message.flags);
        }
        else if(request_message.action === 'closedir')
        {
            response_message = await local_closedir.call(self, request_message.fd);
        }
        else if(request_message.action === 'readdir')
        {
            response_message = await local_readdir.call(self, request_message.path);
        }
        else if(request_message.action === 'getattr')
        {
            response_message = await local_getattr.call(self, request_message.path);
        }
        else if(request_message.action === 'open')
        {
            response_message = await local_open.call(self, request_message.path, request_message.flags);
        }
        else if(request_message.action === 'read')
        {
            response_message = await local_read.call(self, request_message.fd, request_message.length, request_message.position);
        }
        else if(request_message.action === 'release')
        {
            response_message = await local_release.call(self, request_message.fd);
        }
        else if(request_message.action === 'sync')
        {
            response_message = await local_sync.call(self, request_message.fd);
        }
        else if(request_message.action === 'write')
        {
            response_message = await local_write.call(self, request_message.fd, request_message.buffer, request_message.length, request_message.position);
        }
        else if(request_message.action === 'truncate')
        {
            response_message = await local_truncate.call(self, request_message.fd, request_message.size);
        }
        else if(request_message.action === 'create')
        {
            response_message = await local_create.call(self, request_message.path, request_message.mode);
        }
        else if(request_message.action === 'utimens')
        {
            response_message = await local_utimens.call(self, request_message.path, request_message.atime, request_message.mtime);
        }
        else if(request_message.action === 'unlink')
        {
            response_message = await local_unlink.call(self, request_message.path);
        }
        else if(request_message.action === 'rename')
        {
            response_message = await local_rename.call(self, request_message.oldpath, request_message.newpath);
        }
        else if(request_message.action === 'symlink')
        {
            response_message = await local_symlink.call(self, request_message.target, request_message.linkpath);
        }
        else if(request_message.action === 'mkdir')
        {
            response_message = await local_mkdir.call(self, request_message.path, request_message.mode);
        }
        else if(request_message.action === 'rmdir')
        {
            response_message = await local_rmdir.call(self, request_message.path);
        }
        else if(request_message.action === 'readlink')
        {
            response_message = await local_readlink.call(self, request_message.path);
        }
        else if(request_message.action === 'access')
        {
            response_message = await local_access.call(self, request_message.path, request_message.mode);
        }
        
        // send a response with same id
        socket.write(JSON.stringify({
            id: response_id,
            message: response_message
        }) + '\n');
    });
}

module.exports = {
    create: async function create(options) // must at least give: path, address, port
    {
        const self = new events();
        
        self._next_fd = function()
        {
            var i = 0;
            while(this._handles[++i]) {}
            return i;
        };
        self._handles = {};
        self._handles_lookup_path = {};
        self._rootPath = options.path;
        
        self._server = net.createServer();
        self._server.on('close', function()
        {
            self.emit('close');
        });
        self._server.on('error', function(err)
        {
            self.emit('error', err);
        });
        self._server.on('connection', function(socket)
        {
            local_socket.call(self, socket);
        });
        
        self.close = function()
        {
            self._server.close();
        };
        
        return new Promise(function(resolve, reject)
        {
            var hasResolved = false;
            self._server.once('error', function(err)
            {
                if(hasResolved) return;
                
                reject(err);
            });
            self._server.once('listening', function()
            {
                hasResolved = true;
                resolve(self);
            });
            self._server.listen({
                host: options.host,
                port: options.port
            });
        });
    }
};
