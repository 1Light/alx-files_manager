import { v4 as uuidv4 } from 'uuid';
import RedisClient from '../utils/redis';
import DBClient from '../utils/db';
import { ObjectId } from 'mongodb';
import fs from 'fs';
import mime from 'mime-types';
import Bull from 'bull';

class FilesController {
  static async authenticate(request, response) {
    const token = request.header('X-Token') || null;
    if (!token) return response.status(401).send({ error: 'Unauthorized' });

    const redisToken = await RedisClient.get(`auth_${token}`);
    if (!redisToken) return response.status(401).send({ error: 'Unauthorized' });

    const user = await DBClient.db.collection('users').findOne({ _id: ObjectId(redisToken) });
    if (!user) return response.status(401).send({ error: 'Unauthorized' });

    return user;
  }

  static async postUpload(request, response) {
    const fileQueue = new Bull('fileQueue');
    const user = await FilesController.authenticate(request, response);
    if (!user) return;

    const { name: fileName, type: fileType, data: fileData, isPublic = false, parentId = 0 } = request.body;

    if (!fileName) return response.status(400).send({ error: 'Missing name' });
    if (!fileType || !['folder', 'file', 'image'].includes(fileType)) return response.status(400).send({ error: 'Missing type' });
    if (!fileData && ['file', 'image'].includes(fileType)) return response.status(400).send({ error: 'Missing data' });

    let fileParentId = parentId === '0' ? 0 : parentId;
    if (fileParentId !== 0) {
      const parentFile = await DBClient.db.collection('files').findOne({ _id: ObjectId(fileParentId) });
      if (!parentFile) return response.status(400).send({ error: 'Parent not found' });
      if (parentFile.type !== 'folder') return response.status(400).send({ error: 'Parent is not a folder' });
    }

    const fileDataDb = {
      userId: user._id,
      name: fileName,
      type: fileType,
      isPublic: fileIsPublic,
      parentId: fileParentId,
    };

    if (fileType === 'folder') {
      await DBClient.db.collection('files').insertOne(fileDataDb);
      return response.status(201).send(fileDataDb);
    }

    const pathDir = process.env.FOLDER_PATH || '/tmp/files_manager';
    const fileUuid = uuidv4();
    const buff = Buffer.from(fileData, 'base64');
    const pathFile = `${pathDir}/${fileUuid}`;

    try {
      await fs.promises.mkdir(pathDir, { recursive: true });
      await fs.promises.writeFile(pathFile, buff);
    } catch (error) {
      return response.status(400).send({ error: error.message });
    }

    fileDataDb.localPath = pathFile;
    await DBClient.db.collection('files').insertOne(fileDataDb);

    fileQueue.add({
      userId: fileDataDb.userId,
      fileId: fileDataDb._id,
    });

    return response.status(201).send(fileDataDb);
  }

  static async getShow(request, response) {
    const user = await FilesController.authenticate(request, response);
    if (!user) return;

    const idFile = request.params.id || '';
    const fileDocument = await DBClient.db.collection('files').findOne({ _id: ObjectId(idFile), userId: user._id });
    if (!fileDocument) return response.status(404).send({ error: 'Not found' });

    return response.send(fileDocument);
  }

  static async getIndex(request, response) {
    const user = await FilesController.authenticate(request, response);
    if (!user) return;

    const parentId = request.query.parentId || 0;
    const pagination = parseInt(request.query.page, 10) || 0;

    const aggregationMatch = { parentId };
    const aggregateData = [
      { $match: aggregationMatch },
      { $skip: pagination * 20 },
      { $limit: 20 },
    ];

    const files = await DBClient.db.collection('files').aggregate(aggregateData).toArray();
    return response.send(files);
  }

  static async putPublish(request, response) {
    const user = await FilesController.authenticate(request, response);
    if (!user) return;

    const idFile = request.params.id || '';
    let fileDocument = await DBClient.db.collection('files').findOne({ _id: ObjectId(idFile), userId: user._id });
    if (!fileDocument) return response.status(404).send({ error: 'Not found' });

    await DBClient.db.collection('files').updateOne({ _id: ObjectId(idFile) }, { $set: { isPublic: true } });
    fileDocument.isPublic = true;

    return response.send(fileDocument);
  }

  static async putUnpublish(request, response) {
    const user = await FilesController.authenticate(request, response);
    if (!user) return;

    const idFile = request.params.id || '';
    let fileDocument = await DBClient.db.collection('files').findOne({ _id: ObjectId(idFile), userId: user._id });
    if (!fileDocument) return response.status(404).send({ error: 'Not found' });

    await DBClient.db.collection('files').updateOne({ _id: ObjectId(idFile) }, { $set: { isPublic: false } });
    fileDocument.isPublic = false;

    return response.send(fileDocument);
  }

  static async getFile(request, response) {
    const idFile = request.params.id || '';
    const size = request.query.size || 0;

    const fileDocument = await DBClient.db.collection('files').findOne({ _id: ObjectId(idFile) });
    if (!fileDocument) return response.status(404).send({ error: 'Not found' });

    const { isPublic, userId, type } = fileDocument;
    const token = request.header('X-Token') || null;

    let owner = false;
    if (token) {
      const redisToken = await RedisClient.get(`auth_${token}`);
      if (redisToken) {
        const user = await DBClient.db.collection('users').findOne({ _id: ObjectId(redisToken) });
        owner = user && user._id.toString() === userId.toString();
      }
    }

    if (!isPublic && !owner) return response.status(404).send({ error: 'Not found' });
    if (type === 'folder') return response.status(400).send({ error: 'A folder doesn\'t have content' });

    const realPath = size === 0 ? fileDocument.localPath : `${fileDocument.localPath}_${size}`;

    try {
      const dataFile = await fs.promises.readFile(realPath);
      const mimeType = mime.contentType(fileDocument.name);
      response.setHeader('Content-Type', mimeType);
      return response.send(dataFile);
    } catch (error) {
      return response.status(404).send({ error: 'Not found' });
    }
  }
}

export default FilesController;

