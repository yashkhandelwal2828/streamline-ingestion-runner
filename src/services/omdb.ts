import axios from 'axios';
import dotenv from 'dotenv';

dotenv.config();

const OMDB_API_URL = 'http://www.omdbapi.com/';

const getOmdbClient = () => {
  const apiKey = process.env.OMDB_API_KEY;
  if (!apiKey) {
    throw new Error('Missing OMDB_API_KEY in .env file');
  }

  return axios.create({
    baseURL: OMDB_API_URL,
    params: {
      apikey: apiKey,
    },
  });
};

export const getRatings = async (imdbId: string) => {
  try {
    const response = await getOmdbClient().get('/', {
      params: {
        i: imdbId,
        plot: 'full',
      },
    });
    return response.data;
  } catch (error) {
    console.error(`Error fetching ratings for IMDB ID ${imdbId} from OMDB:`, error);
    throw error;
  }
};
