import React, { useState } from 'react';

interface Recommendation {
  lexeme_id: string;
  lexeme_string: string;
  predicted_recall: string;
}

const WordRecommendation: React.FC = () => {
  const [userId, setUserId] = useState<string>('');
  const [recommendations, setRecommendations] = useState<Recommendation[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  const fetchRecommendations = async () => {
    if (!userId) {
      setError('Please enter a user ID');
      return;
    }
  
    setLoading(true);
    setError(null);
  
    try {
      const response = await fetch(`http://localhost:8000/words-recommendation?user_id=${userId}`);
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      console.log(response)
      
      const contentType = response.headers.get('content-type');
      if (!contentType || !contentType.includes('application/json')) {
        throw new Error('Received non-JSON response from the server.');
      }
  
      const data: Recommendation[] = await response.json();
      setRecommendations(data);
    } catch (err: any) {
      setError(err.message);
      setRecommendations([]);
    } finally {
      setLoading(false);
    }
  };
  

  return (
    <div className="max-w-2xl mx-auto p-6 bg-white shadow-md rounded-lg">
      <h1 className="text-2xl font-bold mb-4 text-center">Word Recommendation Finder</h1>
      
      <div className="flex mb-4">
        <input 
          type="text" 
          value={userId}
          onChange={(e) => setUserId(e.target.value)}
          placeholder="Enter User ID"
          className="flex-grow p-2 border border-gray-300 rounded-l-md focus:outline-none focus:ring-2 focus:ring-blue-500"
        />
        <button 
          onClick={fetchRecommendations}
          disabled={loading}
          className="bg-blue-500 text-white px-4 py-2 rounded-r-md hover:bg-blue-600 transition-colors disabled:opacity-50"
        >
          {loading ? 'Loading...' : 'Get Recommendations'}
        </button>
      </div>

      {error && (
        <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded relative mb-4" role="alert">
          {error}
        </div>
      )}

      {recommendations.length > 0 && (
        <div className="overflow-x-auto">
          <table className="w-full bg-white shadow-md rounded-lg overflow-hidden">
            <thead className="bg-gray-100">
              <tr>
                <th className="p-3 text-left">Lexeme ID</th>
                <th className="p-3 text-left">Lexeme String</th>
                <th className="p-3 text-right">Predicted Recall</th>
              </tr>
            </thead>
            <tbody>
              {recommendations.map((rec, index) => (
                <tr 
                  key={rec.lexeme_id} 
                  className={`${index % 2 === 0 ? 'bg-white' : 'bg-gray-50'} hover:bg-gray-100`}
                >
                  <td className="p-3">{rec.lexeme_id}</td>
                  <td className="p-3">{rec.lexeme_string}</td>
                  <td className="p-3 text-right">
                    <span 
                      className={`px-2 py-1 rounded ${
                        rec.predicted_recall === '1.00' 
                          ? 'bg-green-100 text-green-800' 
                          : 'bg-red-100 text-red-800'
                      }`}
                    >
                      {rec.predicted_recall}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
};

export default WordRecommendation;
