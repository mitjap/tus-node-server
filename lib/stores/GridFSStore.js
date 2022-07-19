const crypto = require('crypto');
const stream = require('stream');

const mongodb = require('mongodb');

const DataStore = require('./DataStore')

class GridFSAppendStream extends stream.Writable {
    constructor({ db, bucketName, chunkSize, file, nextPart }) {
        super();

        this.db = db;
        this.bucketName = bucketName;
        this.chunkSize = chunkSize;
        this.nextPart = nextPart;
        this.file = file;

        this.buffer = Buffer.alloc(this.chunkSize);
        this.pos = 0;
    }

    _construct(callback) {
        callback();
    }

    async _write(chunk, encoding, callback) {
        try {
            if (chunk.length + this.pos < this.buffer.length) {
                this.pos += chunk.copy(this.buffer, this.pos);
                return callback();
            }

            let chunkPos = 0;
            while (chunkPos < chunk.length) {
                const chunkRemaining = chunk.length - chunkPos;
                const bufferRemaining = this.buffer.length - this.pos;

                const numToCopy = Math.min(chunkRemaining, bufferRemaining);

                const bytes = chunk.copy(this.buffer, this.pos, chunkPos, chunkPos + numToCopy);
                chunkPos += bytes;
                this.pos += bytes;

                if (this.pos === this.buffer.length) {
                    await this._flush();
                }
            }

            callback();
        } catch (err) {
            callback(err);
        }
    }

    async _final(callback) {
        try {
            // TODO: write leftover data IIF this is last chunk
            if (this.pos !== 0)
                await this._flush();

            callback();
        } catch (err) {
            callback(err);
        }
    }

    _destroy(err, callback) {
        callback(err);
    }

    async _flush() {
        await this.db.collection(`${this.bucketName}.chunks`).insertOne({ files_id: this.file._id, n: this.nextPart, data: Buffer.from(this.buffer, 0, this.pos) });
        await this.db.collection(`${this.bucketName}.files`).updateOne({ _id: this.file._id }, { $inc: { "metadata.size": this.pos }});

        this.pos = 0;
        this.nextPart += 1;
    }
}


class GridFSStore extends DataStore {
    constructor(options = {}) {
        super(options);

        this.options.db = this.options.db;
        this.options.bucketName = this.options.bucketName;;
        this.options.chunkSize = this.options.chunkSize || 256 * 1024;

        const uri = options.uri;
        const client = new mongodb.MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true, serverApi: mongodb.ServerApiVersion.v1 });

        this.db = client.db(this.options.db);

        this.bucket = new mongodb.GridFSBucket(this.db, { bucketName: options.bucketName });
        this.secret = new Date().toISOString();

        this.extensions = ['creation', 'creation-with-upload', 'termination'];
    }

     async create(file) {
        await this.db.collection(`${this.options.bucketName}.files`).insertOne({
            _id: this._createObjectId(file.id),
            length: 0,
            chunkSize: this.options.chunkSize,
            uploadDate: new Date(),
            metadata: {
                id: file.id,
                size: 0,
                upload_length: file.upload_length,
                upload_defer_length: file.upload_defer_length,
                upload_metadata: file.upload_metadata,
            }
        });

        return file;
    }

    read(file_id) {
        return this.bucket.openDownloadStream(this._createObjectId(file_id));
    }

    async remove(file_id) {
        return this.bucket.delete(this._createObjectId(file_id));
    }

    async write(readable, file_id, offset) {
        const file = await this._getFile(file_id);

        if (offset !== file.metadata.size) {
            throw new Error("invalid offset");
        }

        const nextPart = await this._getNextPartNumber(file_id);

        const uploadStream = new GridFSAppendStream({ db: this.db, chunkSize: this.options.chunkSize, bucketName: this.options.bucketName, file, nextPart });

        await new Promise((resolve, reject) => {
            stream.pipeline(readable, uploadStream, (err) => {
                if (err)
                    return reject(err);
                return resolve();
            });
        });

        return (await this._getFile(file_id)).metadata.size;
    }

    async getOffset(file_id) {
        const file = await this._getFile(file_id);
        return file.metadata;
    }

    _createObjectId(file_id) {
        const hash = crypto.createHmac("md5", this.secret).update(file_id).digest("hex").substring(0, 12);
        return mongodb.ObjectId(hash);
    }

    async _getFile(file_id) {
        return this.db.collection(`${this.options.bucketName}.files`).findOne({ _id: this._createObjectId(file_id) });
    }

    async _getNextPartNumber(file_id) {
        const last = await this.db.collection(`${this.options.bucketName}.chunks`).find({ _id: this._createObjectId(file_id) }, { n: 1 }).sort({ n: -1}).limit(1).next();
        return last ? last + 1 : 0;
    }

}

module.exports = GridFSStore;